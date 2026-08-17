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

} // namespace

SessionSkewEstimator::SessionSkewEstimator(
    const SessionSkewEstimatorConfig& config,
    const metrics::PrometheusConfig& prometheus_config)
    : config_(config)
    , common_mode_baseline_(0)
    , has_common_mode_baseline_(false)
    , newest_grid_cts_(0)
    , prometheus_config_(prometheus_config) {
    memset(slots_, 0, sizeof(slots_));
    memset(rows_, 0, sizeof(rows_));
    memset(cov_, 0, sizeof(cov_));
    memset(cov_valid_, 0, sizeof(cov_valid_));

    for (size_t i = 0; i < MaxSlots; i++) {
        slots_[i].stats = SlotStats();
        for (size_t j = 0; j < MaxSlots; j++) {
            pair_stats_[i][j] = PairStats();
        }
    }

#ifdef ROC_TARGET_PROMETHEUS
    memset(pair_skew_gauge_, 0, sizeof(pair_skew_gauge_));
    memset(pair_corr_gauge_, 0, sizeof(pair_corr_gauge_));
    memset(pair_cov_gauge_, 0, sizeof(pair_cov_gauge_));

    metrics::MetricsScope scope;
    scope.side = metrics::MetricsScope::Side_Send;

    std::shared_ptr<prometheus::Registry> registry = metrics::prometheus_registry();
    const prometheus::Labels no_labels;

    spread_gauge_ = &prometheus::BuildGauge()
                         .Name(metrics::scope_metric_name(scope,
                                                          "playout_spread_seconds"))
                         .Help("Max-min clock-free playout skew across session slots")
                         .Register(*registry)
                         .Add(no_labels);
    spread_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "playout_spread"))
             .Help("Distribution of max-min clock-free playout skew in seconds")
             .Register(*registry)
             .Add(no_labels,
                  metrics::generate_histogram_buckets(
                      prometheus_config_.playout_spread));
    common_mode_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "playout_common_mode_seconds"))
             .Help("Fleet mean queue depth minus its slow baseline")
             .Register(*registry)
             .Add(no_labels);

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
             .Help("Snapshots rejected (grid delta gate, unusable fields)")
             .Register(*registry)
             .Add(no_labels);
#endif // ROC_TARGET_PROMETHEUS
}

#ifdef ROC_TARGET_PROMETHEUS

namespace {

prometheus::Gauge* make_slot_gauge(const char* suffix,
                                   const char* help,
                                   const char* slot_name) {
    metrics::MetricsScope scope;
    scope.side = metrics::MetricsScope::Side_Send;
    scope.set_slot(slot_name);

    return &prometheus::BuildGauge()
                .Name(metrics::scope_metric_name(scope, suffix))
                .Help(help)
                .Register(*metrics::prometheus_registry())
                .Add(metrics::scope_labels(scope));
}

} // namespace

void SessionSkewEstimator::register_slot_metrics_(size_t slot_index) {
    Slot& slot = slots_[slot_index];

    slot.offset_gauge =
        make_slot_gauge("playout_offset_seconds",
                        "Clock-free playout offset vs fleet median", slot.name);
    slot.offset_e2e_gauge =
        make_slot_gauge("playout_offset_e2e_seconds",
                        "E2E-based playout offset vs fleet median", slot.name);
    slot.mapping_error_gauge =
        make_slot_gauge("clock_mapping_error_seconds",
                        "Clock-free minus e2e offset (differential mapping error)",
                        slot.name);
    slot.rms_gauge = make_slot_gauge(
        "playout_offset_rms_seconds", "EWMA RMS of playout offset fluctuations",
        slot.name);
    slot.warp_gauge = make_slot_gauge(
        "recv_warp", "Receiver-reported warp (frequency coefficient - 1)", slot.name);
    slot.target_latency_gauge =
        make_slot_gauge("recv_target_latency_seconds",
                        "Receiver-reported target latency", slot.name);
    slot.snapshot_timestamp_gauge = make_slot_gauge(
        "snapshot_timestamp_seconds",
        "Unix time of last received snapshot (age = time() - this)", slot.name);
    slot.flinch_active_gauge = make_slot_gauge(
        "flinch_active", "1 while a playout offset event is in progress", slot.name);
    slot.flinch_magnitude_gauge =
        make_slot_gauge("flinch_magnitude_seconds",
                        "Peak offset excursion of the last flinch event", slot.name);

    metrics::MetricsScope scope;
    scope.side = metrics::MetricsScope::Side_Send;
    scope.set_slot(slot.name);
    slot.flinch_counter =
        &prometheus::BuildCounter()
             .Name(metrics::scope_metric_name(scope, "flinch_total"))
             .Help("Playout offset events (who flinched)")
             .Register(*metrics::prometheus_registry())
             .Add(metrics::scope_labels(scope));
}

void SessionSkewEstimator::register_pair_metrics_(size_t slot_a, size_t slot_b) {
    metrics::MetricsScope scope;
    scope.side = metrics::MetricsScope::Side_Send;

    std::shared_ptr<prometheus::Registry> registry = metrics::prometheus_registry();
    const prometheus::Labels labels =
        metrics::pair_labels(slots_[slot_a].name, slots_[slot_b].name);

    pair_skew_gauge_[slot_a][slot_b] =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "playout_skew_seconds"))
             .Help("Clock-free playout skew slot_a minus slot_b")
             .Register(*registry)
             .Add(labels);
    pair_corr_gauge_[slot_a][slot_b] =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "playout_corr"))
             .Help("EWMA correlation of playout offset fluctuations")
             .Register(*registry)
             .Add(labels);
    pair_cov_gauge_[slot_a][slot_b] =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "playout_cov_seconds2"))
             .Help("EWMA covariance of playout offset fluctuations")
             .Register(*registry)
             .Add(labels);
}

#else // !ROC_TARGET_PROMETHEUS

void SessionSkewEstimator::register_slot_metrics_(size_t) {
}

void SessionSkewEstimator::register_pair_metrics_(size_t, size_t) {
}

#endif // ROC_TARGET_PROMETHEUS

ssize_t SessionSkewEstimator::register_slot(const char* name) {
    roc_panic_if(!name);

    for (size_t i = 0; i < MaxSlots; i++) {
        if (!slots_[i].used) {
            memset(&slots_[i], 0, sizeof(slots_[i]));
            slots_[i].used = true;
            strncpy(slots_[i].name, name, MaxNameLen - 1);
            slots_[i].stats = SlotStats();

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

    slots_[slot_index].used = false;

    for (size_t r = 0; r < MaxRows; r++) {
        rows_[r].present_mask &= ~(1u << slot_index);
    }
}

const char* SessionSkewEstimator::slot_name(size_t slot_index) const {
    roc_panic_if(slot_index >= MaxSlots);

    return slots_[slot_index].name;
}

size_t SessionSkewEstimator::num_slots() const {
    size_t n = 0;
    for (size_t i = 0; i < MaxSlots; i++) {
        if (slots_[i].used) {
            n++;
        }
    }
    return n;
}

void SessionSkewEstimator::process_snapshot(size_t slot_index,
                                            core::nanoseconds_t grid_cts,
                                            core::nanoseconds_t grid_period,
                                            const SlotSample& sample,
                                            core::nanoseconds_t grid_delta,
                                            core::nanoseconds_t arrival_time) {
    roc_panic_if(slot_index >= MaxSlots);

    if (!slots_[slot_index].used) {
        fleet_.rejected++;
        return;
    }

    slots_[slot_index].stats.last_update = arrival_time;
#ifdef ROC_TARGET_PROMETHEUS
    slots_[slot_index].snapshot_timestamp_gauge->Set((double)arrival_time / 1e9);
#endif

    if (grid_period <= 0) {
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        rejected_counter_->Increment();
#endif
        return;
    }

    if (grid_delta < -config_.max_grid_delta || grid_delta > config_.max_grid_delta) {
        // Position doesn't sit on the grid point: CTS mapping breakage
        // or discontinuity on the receiver.
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        rejected_counter_->Increment();
#endif
        return;
    }

    if (sample.niq_mean < 0 && sample.niq_instant < 0) {
        // Nothing usable to compare.
        fleet_.rejected++;
#ifdef ROC_TARGET_PROMETHEUS
        rejected_counter_->Increment();
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
    uint32_t all_mask = 0;
    for (size_t i = 0; i < MaxSlots; i++) {
        if (slots_[i].used) {
            all_mask |= (1u << i);
        }
    }

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

    uint32_t all_mask = 0;
    for (size_t i = 0; i < MaxSlots; i++) {
        if (slots_[i].used) {
            all_mask |= (1u << i);
        }
    }

    if (n_present < 2) {
        // Nothing cross-receiver to compute.
        if (n_present > 0) {
            fleet_.partial_rows++;
        }
        return;
    }

    if ((row.present_mask & all_mask) == all_mask) {
        fleet_.full_rows++;
#ifdef ROC_TARGET_PROMETHEUS
        rows_full_counter_->Increment();
#endif
    } else {
        fleet_.partial_rows++;
#ifdef ROC_TARGET_PROMETHEUS
        rows_partial_counter_->Increment();
#endif
    }

    // Queue depths at the common position.
    double q[MaxSlots];
    double q_sorted[MaxSlots];
    for (size_t n = 0; n < n_present; n++) {
        const SlotSample& sample = row.samples[present[n]];
        q[n] = ns_2_sec(sample.niq_mean >= 0 ? sample.niq_mean : sample.niq_instant);
        q_sorted[n] = q[n];
    }
    const double q_median = median_(q_sorted, n_present);

    // E2E-based offsets (only slots that reported e2e).
    double e2e[MaxSlots];
    double e2e_sorted[MaxSlots];
    size_t n_e2e = 0;
    for (size_t n = 0; n < n_present; n++) {
        const SlotSample& sample = row.samples[present[n]];
        if (sample.e2e_latency >= 0) {
            e2e[n] = ns_2_sec(sample.e2e_latency);
            e2e_sorted[n_e2e++] = e2e[n];
        } else {
            e2e[n] = -1;
        }
    }
    const double e2e_median = n_e2e >= 2 ? median_(e2e_sorted, n_e2e) : 0;

    // Fleet spread and common mode.
    double q_min = q[0], q_max = q[0], q_sum = 0;
    for (size_t n = 0; n < n_present; n++) {
        q_min = q[n] < q_min ? q[n] : q_min;
        q_max = q[n] > q_max ? q[n] : q_max;
        q_sum += q[n];
    }
    const double q_mean = q_sum / (double)n_present;

    const double cm_alpha = (double)row.grid_period / (double)config_.common_mode_tau;
    if (!has_common_mode_baseline_) {
        common_mode_baseline_ = q_mean;
        has_common_mode_baseline_ = true;
    } else {
        common_mode_baseline_ += cm_alpha * (q_mean - common_mode_baseline_);
    }

    fleet_.valid = true;
    fleet_.spread = q_max - q_min;
    fleet_.common_mode = q_mean - common_mode_baseline_;

#ifdef ROC_TARGET_PROMETHEUS
    spread_gauge_->Set(fleet_.spread);
    spread_histogram_->Observe(fleet_.spread);
    common_mode_gauge_->Set(fleet_.common_mode);
#endif

    // Per-slot offsets and EWMA statistics.
    const double alpha = (double)row.grid_period / (double)config_.stats_tau;

    double offset[MaxSlots];
    double centered[MaxSlots];
    for (size_t n = 0; n < n_present; n++) {
        const size_t i = present[n];
        Slot& slot = slots_[i];
        const SlotSample& sample = row.samples[i];

        offset[n] = q[n] - q_median;

        slot.stats.valid = true;
        slot.stats.offset = offset[n];
        if (e2e[n] >= 0 && n_e2e >= 2) {
            slot.stats.offset_e2e = e2e[n] - e2e_median;
            slot.stats.mapping_error = slot.stats.offset - slot.stats.offset_e2e;
        }
        if (sample.has_warp) {
            slot.stats.warp = (double)sample.warp_ppb / 1e9;
        }
        if (sample.target_latency >= 0) {
            slot.stats.target_latency = ns_2_sec(sample.target_latency);
        }

#ifdef ROC_TARGET_PROMETHEUS
        slot.offset_gauge->Set(slot.stats.offset);
        if (e2e[n] >= 0 && n_e2e >= 2) {
            slot.offset_e2e_gauge->Set(slot.stats.offset_e2e);
            slot.mapping_error_gauge->Set(slot.stats.mapping_error);
        }
        slot.warp_gauge->Set(slot.stats.warp);
        slot.target_latency_gauge->Set(slot.stats.target_latency);
#endif

        if (!slot.has_ewma) {
            slot.ewma_mean = offset[n];
            slot.has_ewma = true;
        } else {
            slot.ewma_mean += alpha * (offset[n] - slot.ewma_mean);
        }
        centered[n] = offset[n] - slot.ewma_mean;

        update_flinch_(i, offset[n], row);

        slot.has_prev_offset = true;
        slot.prev_offset = offset[n];
    }

    // Pairwise skew and EWMA covariance/correlation of fluctuations.
    for (size_t a = 0; a < n_present; a++) {
        for (size_t b = a; b < n_present; b++) {
            const size_t i = present[a];
            const size_t j = present[b];

            const double prod = centered[a] * centered[b];
            if (!cov_valid_[i][j]) {
                cov_[i][j] = prod;
                cov_valid_[i][j] = true;
            } else {
                cov_[i][j] += alpha * (prod - cov_[i][j]);
            }

            if (i != j) {
                PairStats& pair = pair_stats_[i][j];
                pair.valid = true;
                pair.skew = q[a] - q[b];
                pair.cov = cov_[i][j];

                const double var_i = cov_valid_[i][i] ? cov_[i][i] : 0;
                const double var_j = cov_valid_[j][j] ? cov_[j][j] : 0;
                if (var_i > 0 && var_j > 0) {
                    pair.corr = cov_[i][j] / sqrt(var_i * var_j);
                    if (pair.corr > 1) {
                        pair.corr = 1;
                    }
                    if (pair.corr < -1) {
                        pair.corr = -1;
                    }
                }

#ifdef ROC_TARGET_PROMETHEUS
                if (pair_skew_gauge_[i][j]) {
                    pair_skew_gauge_[i][j]->Set(pair.skew);
                    pair_corr_gauge_[i][j]->Set(pair.corr);
                    pair_cov_gauge_[i][j]->Set(pair.cov);
                }
#endif
            }
        }
    }

    // Per-slot RMS from the variance diagonal.
    for (size_t n = 0; n < n_present; n++) {
        const size_t i = present[n];
        if (cov_valid_[i][i] && cov_[i][i] > 0) {
            slots_[i].stats.rms = sqrt(cov_[i][i]);
#ifdef ROC_TARGET_PROMETHEUS
            slots_[i].rms_gauge->Set(slots_[i].stats.rms);
#endif
        }
    }
}

void SessionSkewEstimator::update_flinch_(size_t slot_index,
                                          double offset,
                                          const Row& row) {
    Slot& slot = slots_[slot_index];
    SlotStats& stats = slot.stats;

    const double step_threshold = ns_2_sec(config_.flinch_step);
    const double abs_threshold = ns_2_sec(config_.flinch_abs);
    const double release_band = ns_2_sec(config_.flinch_release_band);

    if (!stats.flinch_active) {
        const bool step_trigger = slot.has_prev_offset
            && fabs(offset - slot.prev_offset) > step_threshold;
        const bool abs_trigger = fabs(offset - slot.ewma_mean) > abs_threshold;

        if (step_trigger || abs_trigger) {
            stats.flinch_active = true;
            stats.flinch_count++;
#ifdef ROC_TARGET_PROMETHEUS
            slot.flinch_active_gauge->Set(1);
            slot.flinch_counter->Increment();
#endif
            slot.flinch_baseline = slot.has_prev_offset ? slot.prev_offset : 0;
            slot.flinch_start_cts = row.grid_cts;
            slot.flinch_hold_ns = 0;
            stats.flinch_magnitude = fabs(offset - slot.flinch_baseline);

            roc_log(LogDebug,
                    "session skew estimator: flinch start: slot=%s offset=%.6f"
                    " baseline=%.6f",
                    slot.name, offset, slot.flinch_baseline);
        }
        return;
    }

    const double excursion = fabs(offset - slot.flinch_baseline);
    if (excursion > stats.flinch_magnitude) {
        stats.flinch_magnitude = excursion;
    }
#ifdef ROC_TARGET_PROMETHEUS
    slot.flinch_magnitude_gauge->Set(stats.flinch_magnitude);
#endif

    if (excursion <= release_band) {
        slot.flinch_hold_ns += row.grid_period;
        if (slot.flinch_hold_ns >= config_.flinch_hold) {
            stats.flinch_active = false;
            stats.flinch_duration =
                ns_2_sec(row.grid_cts - slot.flinch_start_cts - slot.flinch_hold_ns);
            if (stats.flinch_duration < 0) {
                stats.flinch_duration = 0;
            }
#ifdef ROC_TARGET_PROMETHEUS
            slot.flinch_active_gauge->Set(0);
#endif

            roc_log(LogDebug,
                    "session skew estimator: flinch end: slot=%s magnitude=%.6f"
                    " duration=%.3f",
                    slot.name, stats.flinch_magnitude, stats.flinch_duration);
        }
    } else {
        slot.flinch_hold_ns = 0;
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

    if (slot_a < slot_b) {
        stats = pair_stats_[slot_a][slot_b];
    } else {
        stats = pair_stats_[slot_b][slot_a];
        stats.skew = -stats.skew;
    }
    return true;
}

void SessionSkewEstimator::fleet_stats(FleetStats& stats) const {
    stats = fleet_;
}

} // namespace pipeline
} // namespace roc
