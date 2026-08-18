/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_pipeline/session_skew_estimator.h"
#include "roc_core/log.h"
#include "roc_core/panic.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/family.h>
#endif

#include <math.h>
#include <string.h>

namespace roc {
namespace pipeline {

namespace {

double ns_2_sec(core::nanoseconds_t ns) {
    return (double)ns / 1e9;
}

// Median of the first n values (n <= MaxSlots); sorts in place.
double median_(double* values, size_t n) {
    roc_panic_if(n == 0);

    for (size_t i = 1; i < n; i++) {
        const double v = values[i];
        size_t j = i;
        while (j > 0 && values[j - 1] > v) {
            values[j] = values[j - 1];
            j--;
        }
        values[j] = v;
    }

    if (n % 2 == 1) {
        return values[n / 2];
    }
    return (values[n / 2 - 1] + values[n / 2]) / 2;
}

#ifdef ROC_TARGET_PROMETHEUS

// One table row per per-slot gauge: a gauge cannot exist without a
// name, a help text and an enum id that some write site uses.
struct SlotGaugeDef {
    const char* suffix;
    const char* help;
};

const SlotGaugeDef* slot_gauge_defs() {
    static const SlotGaugeDef defs[SessionSkewEstimator::NumSlotGauges] = {
        { "playout_offset_seconds",
          "E2E playout offset against the fleet median (assumes"
          " NTP-synchronized host clocks)" },
        { "playout_offset_rms_seconds",
          "RMS of the slot's queue depth around its own exponential average" },
        { "recv_warp", "Receiver-reported warp (frequency coefficient - 1)" },
        { "recv_target_latency_seconds", "Receiver-reported target latency" },
        { "snapshot_timestamp_seconds",
          "Unix time when the last snapshot was accepted; age = time() - value" },
        { "offset_jump_active", "1 while an offset jump event is in progress" },
        { "offset_jump_magnitude_seconds",
          "Peak offset excursion of the last jump event" },
        { "offset_jump_duration_seconds",
          "Duration of the last completed jump event" },
    };
    return defs;
}

#endif // ROC_TARGET_PROMETHEUS

} // namespace

SessionSkewEstimator::SessionSkewEstimator(
    const SessionSkewEstimatorConfig& config,
    const metrics::PrometheusConfig& prometheus_config)
    : config_(config)
    , used_mask_(0)
    , jump_step_sec_(ns_2_sec(config.jump_step))
    , jump_abs_sec_(ns_2_sec(config.jump_abs))
    , jump_release_sec_(ns_2_sec(config.jump_release_band))
    , newest_grid_cts_(0)
    , metrics_enabled_(prometheus_config.port > 0) {
    memset(slots_, 0, sizeof(slots_));
    memset(rows_, 0, sizeof(rows_));
    memset(pairs_, 0, sizeof(pairs_));

#ifdef ROC_TARGET_PROMETHEUS
    memset(slot_gauge_families_, 0, sizeof(slot_gauge_families_));
    memset(pair_gauge_families_, 0, sizeof(pair_gauge_families_));
    jump_counter_family_ = NULL;
    fleet_mean_gauge_ = NULL;
    fleet_mean_histogram_ = NULL;
    stddev_gauge_ = NULL;
    stddev_histogram_ = NULL;
    spread_gauge_ = NULL;
    spread_histogram_ = NULL;
    rows_full_counter_ = NULL;
    rows_partial_counter_ = NULL;
    rejected_counter_ = NULL;

    if (!metrics_enabled_) {
        // No exposer will ever serve these series; registering them
        // would only spend memory and, with several sinks in one
        // process, alias the unlabeled fleet series between them.
        return;
    }

    metrics::MetricsScope scope;
    scope.side = metrics::MetricsScope::Side_Send;

    std::shared_ptr<prometheus::Registry> registry = metrics::prometheus_registry();
    const prometheus::Labels no_labels;

    for (size_t g = 0; g < NumSlotGauges; g++) {
        slot_gauge_families_[g] =
            &prometheus::BuildGauge()
                 .Name(metrics::scope_metric_name(scope, slot_gauge_defs()[g].suffix))
                 .Help(slot_gauge_defs()[g].help)
                 .Register(*registry);
    }
    jump_counter_family_ =
        &prometheus::BuildCounter()
             .Name(metrics::scope_metric_name(scope, "offset_jump_total"))
             .Help("Offset jump events per slot (trigger: offset step above"
                   " the configured threshold)")
             .Register(*registry);

    static const char* pair_suffixes[3] = { "playout_skew_seconds", "playout_corr",
                                            "playout_cov_seconds2" };
    static const char* pair_helps[3] = {
        "E2E playout skew, slot_a minus slot_b",
        "Correlation of the two slots' queue-depth (buffer margin)"
        " fluctuations, a transport diagnostic, not a sync metric; mean"
        " and covariance are exponential averages",
        "Covariance of the two slots' queue-depth fluctuations; mean and"
        " covariance are exponential averages",
    };
    for (size_t g = 0; g < 3; g++) {
        pair_gauge_families_[g] = &prometheus::BuildGauge()
                                       .Name(metrics::scope_metric_name(
                                           scope, pair_suffixes[g]))
                                       .Help(pair_helps[g])
                                       .Register(*registry);
    }

    // Fleet cross-section statistics: mean, population stddev and
    // max-min of the e2e latencies at one grid instant, over the slots
    // that reported e2e. Gauges carry the last row; histograms
    // aggregate the rows over time. The stddev histogram shares the
    // playout_spread bucket bounds; the mean has its own (it lives near
    // the target latency, two decades above the spreads).
    fleet_mean_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "playout_fleet_mean_seconds"))
             .Help("Mean e2e latency across session slots at a common"
                   " stream position")
             .Register(*registry)
             .Add(no_labels);
    fleet_mean_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "playout_fleet_mean"))
             .Help("Distribution of the mean e2e latency across session"
                   " slots, in seconds; one sample per grid row")
             .Register(*registry)
             .Add(no_labels,
                  metrics::generate_histogram_buckets(
                      prometheus_config.playout_fleet_mean));
    stddev_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "playout_stddev_seconds"))
             .Help("Population standard deviation of e2e latency across"
                   " session slots at a common stream position")
             .Register(*registry)
             .Add(no_labels);
    stddev_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "playout_stddev"))
             .Help("Distribution of the e2e latency standard deviation"
                   " across session slots, in seconds; one sample per grid row")
             .Register(*registry)
             .Add(no_labels,
                  metrics::generate_histogram_buckets(prometheus_config.playout_spread));
    spread_gauge_ = &prometheus::BuildGauge()
                         .Name(metrics::scope_metric_name(scope,
                                                          "playout_spread_seconds"))
                         .Help("Max-min e2e playout skew across session slots")
                         .Register(*registry)
                         .Add(no_labels);
    spread_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "playout_spread"))
             .Help("Distribution of max-min e2e playout skew in seconds")
             .Register(*registry)
             .Add(no_labels,
                  metrics::generate_histogram_buckets(prometheus_config.playout_spread));

    auto& rows_family =
        prometheus::BuildCounter()
            .Name(metrics::scope_metric_name(scope, "snapshot_rows_total"))
            .Help("Finalized snapshot grid rows")
            .Register(*registry);
    rows_full_counter_ = &rows_family.Add({ { "completeness", "full" } });
    rows_partial_counter_ = &rows_family.Add({ { "completeness", "partial" } });

    rejected_counter_ =
        &prometheus::BuildCounter()
             .Name(metrics::scope_metric_name(scope, "snapshot_rejected_total"))
             .Help("Snapshots rejected (validity gates)")
             .Register(*registry)
             .Add(no_labels);
#endif // ROC_TARGET_PROMETHEUS
}

#ifdef ROC_TARGET_PROMETHEUS

void SessionSkewEstimator::register_slot_metrics_(size_t slot_index) {
    if (!metrics_enabled_) {
        return;
    }

    Slot& slot = slots_[slot_index];

    metrics::MetricsScope scope;
    scope.side = metrics::MetricsScope::Side_Send;
    scope.set_slot(slot.name);
    const prometheus::Labels labels = metrics::scope_labels(scope);

    for (size_t g = 0; g < NumSlotGauges; g++) {
        slot.gauges[g] = &slot_gauge_families_[g]->Add(labels);
    }
    slot.jump_counter = &jump_counter_family_->Add(labels);
}

void SessionSkewEstimator::register_pair_metrics_(size_t slot_a, size_t slot_b) {
    if (!metrics_enabled_) {
        return;
    }

    Pair& pair = pairs_[slot_a][slot_b];
    const prometheus::Labels labels =
        metrics::pair_labels(slots_[slot_a].name, slots_[slot_b].name);

    pair.skew_gauge = &pair_gauge_families_[0]->Add(labels);
    pair.corr_gauge = &pair_gauge_families_[1]->Add(labels);
    pair.cov_gauge = &pair_gauge_families_[2]->Add(labels);
}

void SessionSkewEstimator::remove_slot_metrics_(size_t slot_index) {
    if (!metrics_enabled_) {
        return;
    }

    Slot& slot = slots_[slot_index];

    for (size_t g = 0; g < NumSlotGauges; g++) {
        if (slot.gauges[g]) {
            slot_gauge_families_[g]->Remove(slot.gauges[g]);
            slot.gauges[g] = NULL;
        }
    }
    if (slot.jump_counter) {
        jump_counter_family_->Remove(slot.jump_counter);
        slot.jump_counter = NULL;
    }

    for (size_t j = 0; j < MaxSlots; j++) {
        Pair& lo = pairs_[j < slot_index ? j : slot_index][j < slot_index ? slot_index : j];
        if (j != slot_index && lo.skew_gauge) {
            pair_gauge_families_[0]->Remove(lo.skew_gauge);
            pair_gauge_families_[1]->Remove(lo.corr_gauge);
            pair_gauge_families_[2]->Remove(lo.cov_gauge);
            lo.skew_gauge = lo.corr_gauge = lo.cov_gauge = NULL;
        }
    }
}

#else // !ROC_TARGET_PROMETHEUS

void SessionSkewEstimator::register_slot_metrics_(size_t) {
}

void SessionSkewEstimator::register_pair_metrics_(size_t, size_t) {
}

void SessionSkewEstimator::remove_slot_metrics_(size_t) {
}

#endif // ROC_TARGET_PROMETHEUS

// Clears the accumulated statistics tied to a slot index, so a future
// occupant of the index cannot inherit a departed slot's state.
void SessionSkewEstimator::clear_slot_state_(size_t slot_index) {
    for (size_t j = 0; j < MaxSlots; j++) {
        Pair& lo =
            pairs_[j < slot_index ? j : slot_index][j < slot_index ? slot_index : j];
        lo.valid = false;
        lo.skew = 0;
        lo.cov = stat::ExpAvg();
    }
    Pair& diag = pairs_[slot_index][slot_index];
    diag.valid = false;
    diag.skew = 0;
    diag.cov = stat::ExpAvg();
}

ssize_t SessionSkewEstimator::register_slot(const char* name) {
    roc_panic_if(!name);

    for (size_t i = 0; i < MaxSlots; i++) {
        if (!slots_[i].used) {
            memset(&slots_[i], 0, sizeof(slots_[i]));
            slots_[i].used = true;
            slots_[i].stats = SlotStats();
            used_mask_ |= (1u << i);
            clear_slot_state_(i);

            // Unique label, always: an empty label would produce an
            // UNLABELED series, and a duplicate label the SAME series
            // object, so several slots would silently write one gauge.
            if (name[0] == '\0') {
                snprintf(slots_[i].name, MaxNameLen, "slot%u", (unsigned)i);
            } else {
                strncpy(slots_[i].name, name, MaxNameLen - 1);
            }
            for (size_t j = 0; j < MaxSlots; j++) {
                if (j != i && slots_[j].used
                    && strcmp(slots_[j].name, slots_[i].name) == 0) {
                    char base[MaxNameLen];
                    strncpy(base, slots_[i].name, MaxNameLen - 1);
                    base[MaxNameLen - 1] = '\0';
                    snprintf(slots_[i].name, MaxNameLen, "%.*s_%u", MaxNameLen - 8,
                             base, (unsigned)i);
                    break;
                }
            }

            register_slot_metrics_(i);
            for (size_t j = 0; j < MaxSlots; j++) {
                if (j != i && slots_[j].used) {
                    register_pair_metrics_(j < i ? j : i, j < i ? i : j);
                }
            }

            return (ssize_t)i;
        }
    }

    roc_log(LogError, "session skew estimator: too many slots");
    return -1;
}

void SessionSkewEstimator::unregister_slot(size_t slot_index) {
    roc_panic_if(slot_index >= MaxSlots);

    remove_slot_metrics_(slot_index);
    clear_slot_state_(slot_index);

    slots_[slot_index].used = false;
    used_mask_ &= ~(1u << slot_index);

    for (size_t r = 0; r < MaxRows; r++) {
        rows_[r].present_mask &= ~(1u << slot_index);
    }
}

void SessionSkewEstimator::process_snapshot(size_t slot_index,
                                            core::nanoseconds_t grid_cts,
                                            core::nanoseconds_t grid_period,
                                            const packet::StreamSnapshot& sample,
                                            core::nanoseconds_t grid_delta,
                                            core::nanoseconds_t arrival_time) {
    roc_panic_if(slot_index >= MaxSlots);

    if (!slots_[slot_index].used) {
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rejected_counter_->Increment();
        }
#endif
        return;
    }

    if (grid_period <= 0) {
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rejected_counter_->Increment();
        }
#endif
        return;
    }

    if (grid_delta < -config_.max_grid_delta || grid_delta > config_.max_grid_delta) {
        // Position doesn't sit on the grid point: CTS mapping breakage
        // or discontinuity on the receiver.
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rejected_counter_->Increment();
        }
#endif
        return;
    }

    if (sample.niq_mean < 0) {
        // No interval mean. The correlation statistics compare interval
        // means across slots; mixing in an instantaneous value would
        // compare two different quantities.
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rejected_counter_->Increment();
        }
#endif
        return;
    }

    if (grid_cts > arrival_time + config_.max_future_grid) {
        // A grid point is a capture instant the receiver already
        // played; it cannot sit far in the future of the local clock.
        // This bound protects newest_grid_cts_, which only grows.
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rejected_counter_->Increment();
        }
#endif
        return;
    }

    Row* row = find_or_create_row_(grid_cts, grid_period);
    if (!row) {
        // Row already finalized (late duplicate): idempotent no-op.
        return;
    }

    if (row->present_mask & (1u << slot_index)) {
        // Duplicate entry for this slot: idempotent no-op.
        return;
    }

    row->samples[slot_index] = sample;
    row->present_mask |= (1u << slot_index);

    // Freshness marks only on ACCEPTED new snapshots, so a slot whose
    // snapshots are all rejected (or replayed) reads as stale.
    slots_[slot_index].stats.last_update = arrival_time;
#ifdef ROC_TARGET_PROMETHEUS
    if (metrics_enabled_) {
        slots_[slot_index].gauges[Gauge_SnapshotTimestamp]->Set(ns_2_sec(arrival_time));
    }
#endif

    if (grid_cts > newest_grid_cts_) {
        newest_grid_cts_ = grid_cts;
    }

    finalize_ready_rows_();
}

SessionSkewEstimator::Row*
SessionSkewEstimator::find_or_create_row_(core::nanoseconds_t grid_cts,
                                          core::nanoseconds_t grid_period) {
    for (size_t r = 0; r < MaxRows; r++) {
        Row& row = rows_[r];
        if (row.used && row.grid_cts == grid_cts) {
            return row.finalized ? NULL : &row;
        }
    }

    Row* oldest = NULL;
    for (size_t r = 0; r < MaxRows; r++) {
        Row& row = rows_[r];
        if (!row.used) {
            oldest = &row;
            break;
        }
        if (!oldest || row.grid_cts < oldest->grid_cts) {
            oldest = &row;
        }
    }

    if (grid_cts <= newest_grid_cts_
        && newest_grid_cts_ - grid_cts
            > (core::nanoseconds_t)config_.late_row_periods * grid_period * 2) {
        // Far in the past and its row is gone: stale resend.
        return NULL;
    }

    roc_panic_if(!oldest);

    memset(oldest, 0, sizeof(*oldest));
    oldest->used = true;
    oldest->grid_cts = grid_cts;
    oldest->grid_period = grid_period;

    return oldest;
}

void SessionSkewEstimator::finalize_ready_rows_() {
    const uint32_t all_mask = used_mask_;

    for (size_t r = 0; r < MaxRows; r++) {
        Row& row = rows_[r];
        if (!row.used || row.finalized) {
            continue;
        }

        const bool complete = all_mask != 0 && (row.present_mask & all_mask) == all_mask;
        const bool late = newest_grid_cts_ - row.grid_cts
            >= (core::nanoseconds_t)config_.late_row_periods * row.grid_period;

        if (complete || late) {
            finalize_row_(row);
        }
    }
}

void SessionSkewEstimator::finalize_row_(Row& row) {
    row.finalized = true;

    size_t present[MaxSlots];
    size_t n_present = 0;
    for (size_t i = 0; i < MaxSlots; i++) {
        if (slots_[i].used && (row.present_mask & (1u << i))) {
            present[n_present++] = i;
        }
    }

    const uint32_t all_mask = used_mask_;

    if (n_present < 2) {
        // Nothing cross-receiver to compute.
        if (n_present > 0) {
            fleet_.partial_rows++;
#ifdef ROC_TARGET_PROMETHEUS
            if (metrics_enabled_) {
                rows_partial_counter_->Increment();
            }
#endif
        }
        return;
    }

    if ((row.present_mask & all_mask) == all_mask) {
        fleet_.full_rows++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rows_full_counter_->Increment();
        }
#endif
    } else {
        fleet_.partial_rows++;
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            rows_partial_counter_->Increment();
        }
#endif
    }

    // Queue depths at the common position: the correlation base.
    double q[MaxSlots];
    for (size_t n = 0; n < n_present; n++) {
        q[n] = ns_2_sec(row.samples[present[n]].niq_mean);
    }

    // E2E latencies at the common position: the sync base. Sync
    // statistics assume NTP-synchronized host clocks, under which e2e
    // differences between receivers equal playout-timing differences.
    // A slot without an RTCP clock mapping reports no e2e value and is
    // absent from this row's sync statistics; a row needs two e2e
    // slots for a cross-receiver comparison.
    double e2e[MaxSlots];
    bool has_e2e[MaxSlots];
    double e2e_sorted[MaxSlots];
    size_t n_sync = 0;
    for (size_t n = 0; n < n_present; n++) {
        const packet::StreamSnapshot& sample = row.samples[present[n]];
        has_e2e[n] = sample.e2e_latency >= 0;
        e2e[n] = has_e2e[n] ? ns_2_sec(sample.e2e_latency) : 0;
        if (has_e2e[n]) {
            e2e_sorted[n_sync++] = e2e[n];
        }
    }
    const bool sync_row = n_sync >= 2;
    const double e2e_median = sync_row ? median_(e2e_sorted, n_sync) : 0;

    if (sync_row) {
        // Fleet cross-section statistics over the e2e slots. median_()
        // sorted the subset, so the extremes sit at its ends.
        double l_sum = 0;
        for (size_t n = 0; n < n_sync; n++) {
            l_sum += e2e_sorted[n];
        }
        const double l_mean = l_sum / (double)n_sync;

        double l_var = 0;
        for (size_t n = 0; n < n_sync; n++) {
            l_var += (e2e_sorted[n] - l_mean) * (e2e_sorted[n] - l_mean);
        }
        l_var /= (double)n_sync;

        fleet_.valid = true;
        fleet_.mean = l_mean;
        fleet_.stddev = sqrt(l_var);
        fleet_.spread = e2e_sorted[n_sync - 1] - e2e_sorted[0];

#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            fleet_mean_gauge_->Set(fleet_.mean);
            fleet_mean_histogram_->Observe(fleet_.mean);
            stddev_gauge_->Set(fleet_.stddev);
            stddev_histogram_->Observe(fleet_.stddev);
            spread_gauge_->Set(fleet_.spread);
            spread_histogram_->Observe(fleet_.spread);
        }
#endif
    }

    // Per-slot offsets and EWMA statistics.
    const double alpha = (double)row.grid_period / (double)config_.stats_tau;

    double centered[MaxSlots];
    bool has_centered[MaxSlots];
    for (size_t n = 0; n < n_present; n++) {
        const size_t i = present[n];
        Slot& slot = slots_[i];
        const packet::StreamSnapshot& sample = row.samples[i];

        slot.stats.valid = true;
        if (sample.has_warp) {
            slot.stats.warp = (double)sample.warp_ppb / 1e9;
        }
        if (sample.target_latency >= 0) {
            slot.stats.target_latency = ns_2_sec(sample.target_latency);
        }

#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            slot.gauges[Gauge_Warp]->Set(slot.stats.warp);
            slot.gauges[Gauge_TargetLatency]->Set(slot.stats.target_latency);
        }
#endif

        if (sync_row && has_e2e[n]) {
            const double offset = e2e[n] - e2e_median;

            slot.stats.offset = offset;

#ifdef ROC_TARGET_PROMETHEUS
            if (metrics_enabled_) {
                slot.gauges[Gauge_Offset]->Set(slot.stats.offset);
            }
#endif

            // The event detector compares against the mean BEFORE this
            // row's offset enters it; otherwise the trigger references
            // itself and desensitizes by alpha.
            update_jump_(i, offset, row);

            slot.offset_mean.update(alpha, offset);

            slot.has_prev_offset = true;
            slot.prev_offset = offset;
        }

        // Covariance base: the slot's own queue-depth average. A fleet
        // reference (median or mean) mixes the slots' signals and
        // fabricates anti-correlation on the minority side of any
        // correlated group; own-mean centering measures each pair
        // alone. A genuinely global cause then shows in ALL pairs,
        // which is the honest reading.
        // Centering uses the mean BEFORE this row's sample enters it:
        // the residual keeps its full size, and a slot's first row only
        // seeds the mean and contributes no product.
        if (slot.q_mean.has()) {
            centered[n] = q[n] - slot.q_mean.get();
            has_centered[n] = true;
        } else {
            centered[n] = 0;
            has_centered[n] = false;
        }
        slot.q_mean.update(alpha, q[n]);
    }

    // Pairwise statistics: e2e skew for pairs where both slots reported
    // e2e; queue-depth covariance/correlation for all co-present pairs.
    for (size_t a = 0; a < n_present; a++) {
        for (size_t b = a; b < n_present; b++) {
            const size_t i = present[a];
            const size_t j = present[b];

            Pair& pair = pairs_[i][j];

            if (has_centered[a] && has_centered[b]) {
                pair.cov.update(alpha, centered[a] * centered[b]);
            }

            if (i != j) {
                pair.valid = true;
                if (has_e2e[a] && has_e2e[b]) {
                    pair.skew = e2e[a] - e2e[b];
                }

#ifdef ROC_TARGET_PROMETHEUS
                if (metrics_enabled_ && pair.skew_gauge) {
                    if (has_e2e[a] && has_e2e[b]) {
                        pair.skew_gauge->Set(pair.skew);
                    }
                    pair.corr_gauge->Set(pair_corr_(i, j));
                    pair.cov_gauge->Set(pair.cov.has() ? pair.cov.get() : 0);
                }
#endif
            }
        }
    }

    // Per-slot RMS from the variance diagonal.
    for (size_t n = 0; n < n_present; n++) {
        const size_t i = present[n];
        if (pairs_[i][i].cov.has() && pairs_[i][i].cov.get() > 0) {
            slots_[i].stats.rms = sqrt(pairs_[i][i].cov.get());
#ifdef ROC_TARGET_PROMETHEUS
            if (metrics_enabled_) {
                slots_[i].gauges[Gauge_Rms]->Set(slots_[i].stats.rms);
            }
#endif
        }
    }
}

// Correlation from the covariance triangle; [-1; 1], zero when either
// variance is not yet established.
double SessionSkewEstimator::pair_corr_(size_t slot_a, size_t slot_b) const {
    const stat::ExpAvg& var_a = pairs_[slot_a][slot_a].cov;
    const stat::ExpAvg& var_b = pairs_[slot_b][slot_b].cov;
    const stat::ExpAvg& cov = pairs_[slot_a][slot_b].cov;
    if (!var_a.has() || !var_b.has() || !cov.has() || var_a.get() <= 0
        || var_b.get() <= 0) {
        return 0;
    }
    double corr = cov.get() / sqrt(var_a.get() * var_b.get());
    if (corr > 1) {
        corr = 1;
    }
    if (corr < -1) {
        corr = -1;
    }
    return corr;
}

void SessionSkewEstimator::update_jump_(size_t slot_index,
                                          double offset,
                                          const Row& row) {
    Slot& slot = slots_[slot_index];
    SlotStats& stats = slot.stats;

    const double step_threshold = jump_step_sec_;
    const double abs_threshold = jump_abs_sec_;
    const double release_band = jump_release_sec_;

    if (!stats.jump_active) {
        const bool step_trigger = slot.has_prev_offset
            && fabs(offset - slot.prev_offset) > step_threshold;
        const double mean_ref = slot.offset_mean.has() ? slot.offset_mean.get() : 0;
        const bool abs_trigger = fabs(offset - mean_ref) > abs_threshold;

        if (step_trigger || abs_trigger) {
            stats.jump_active = true;
            stats.jump_count++;
#ifdef ROC_TARGET_PROMETHEUS
            if (metrics_enabled_) {
                slot.gauges[Gauge_JumpActive]->Set(1);
                slot.jump_counter->Increment();
            }
#endif
            slot.jump_baseline = slot.has_prev_offset ? slot.prev_offset : 0;
            slot.jump_start_cts = row.grid_cts;
            slot.jump_hold_ns = 0;
            stats.jump_magnitude = fabs(offset - slot.jump_baseline);

            roc_log(LogDebug,
                    "session skew estimator: offset jump start: slot=%s offset=%.6f"
                    " baseline=%.6f",
                    slot.name, offset, slot.jump_baseline);
        }
        return;
    }

    if (row.grid_cts - slot.jump_start_cts >= config_.jump_max_duration) {
        // The offset settled at a new level instead of returning: close
        // the event; the new level is the new normal.
        stats.jump_active = false;
        stats.jump_duration = ns_2_sec(config_.jump_max_duration);
#ifdef ROC_TARGET_PROMETHEUS
        if (metrics_enabled_) {
            slot.gauges[Gauge_JumpActive]->Set(0);
            slot.gauges[Gauge_JumpDuration]->Set(stats.jump_duration);
        }
#endif
        roc_log(LogDebug,
                "session skew estimator: offset jump timeout (level shift):"
                " slot=%s magnitude=%.6f",
                slot.name, stats.jump_magnitude);
        return;
    }

    const double excursion = fabs(offset - slot.jump_baseline);
    if (excursion > stats.jump_magnitude) {
        stats.jump_magnitude = excursion;
    }
#ifdef ROC_TARGET_PROMETHEUS
    if (metrics_enabled_) {
        slot.gauges[Gauge_JumpMagnitude]->Set(stats.jump_magnitude);
    }
#endif

    if (excursion <= release_band) {
        if (slot.jump_hold_ns == 0) {
            // First row back inside the band: the event ended here.
            slot.jump_end_cts = row.grid_cts;
        }
        slot.jump_hold_ns += row.grid_period;
        if (slot.jump_hold_ns >= config_.jump_hold) {
            stats.jump_active = false;
            stats.jump_duration = ns_2_sec(slot.jump_end_cts - slot.jump_start_cts);
#ifdef ROC_TARGET_PROMETHEUS
            if (metrics_enabled_) {
                slot.gauges[Gauge_JumpActive]->Set(0);
                slot.gauges[Gauge_JumpDuration]->Set(stats.jump_duration);
            }
#endif

            roc_log(LogDebug,
                    "session skew estimator: offset jump end: slot=%s magnitude=%.6f"
                    " duration=%.3f",
                    slot.name, stats.jump_magnitude, stats.jump_duration);
        }
    } else {
        slot.jump_hold_ns = 0;
    }
}

bool SessionSkewEstimator::slot_stats(size_t slot_index, SlotStats& stats) const {
    roc_panic_if(slot_index >= MaxSlots);

    if (!slots_[slot_index].used) {
        return false;
    }

    stats = slots_[slot_index].stats;
    return true;
}

bool SessionSkewEstimator::pair_stats(size_t slot_a,
                                      size_t slot_b,
                                      PairStats& stats) const {
    roc_panic_if(slot_a >= MaxSlots);
    roc_panic_if(slot_b >= MaxSlots);
    roc_panic_if(slot_a == slot_b);

    if (!slots_[slot_a].used || !slots_[slot_b].used) {
        return false;
    }

    const size_t lo = slot_a < slot_b ? slot_a : slot_b;
    const size_t hi = slot_a < slot_b ? slot_b : slot_a;

    stats.valid = pairs_[lo][hi].valid;
    stats.skew = slot_a < slot_b ? pairs_[lo][hi].skew : -pairs_[lo][hi].skew;
    stats.cov = pairs_[lo][hi].cov.has() ? pairs_[lo][hi].cov.get() : 0;
    stats.corr = pair_corr_(lo, hi);
    return true;
}

void SessionSkewEstimator::fleet_stats(FleetStats& stats) const {
    stats = fleet_;
}

} // namespace pipeline
} // namespace roc
