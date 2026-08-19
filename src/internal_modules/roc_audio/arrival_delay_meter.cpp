/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_audio/arrival_delay_meter.h"
#include "roc_core/panic.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/family.h>
#endif

#include <math.h>

namespace roc {
namespace audio {

namespace {

// No events are opened before the variance estimate accumulated this
// much of its steady-state weight. One half means the estimate carries
// at least half of the information a converged estimate would; it is
// reached after ln(2) * sigma_tau (about 0.7 * sigma_tau) of stream
// time, so the conservative window decays within sigma_tau.
// The weight gate is paired with a well-definedness gate: events also
// need a positive variance estimate, since with sigma = 0 the
// thresholds are not defined.
const double MinVarianceWeight = 0.5;

} // namespace

ArrivalDelayMeter::ArrivalDelayMeter(const ArrivalDelayMeterConfig& config,
                                     core::IArena& arena)
    : config_(config)
    , floor_window_(arena, config.floor_window)
    , peak_window_(arena, config.peak_window)
    , stream_clock_(0)
    , event_open_(false)
    , event_height_(0)
    , event_duration_(0)
    , has_event_gap_(false)
    , event_gap_(0)
    , has_prev_close_(false)
    , prev_close_clock_(0)
    , interval_dev_sum_(0)
    , interval_dev_max_(0)
    , interval_packets_(0)
    , interval_events_(0)
    , init_status_(status::NoStatus) {
    if (!floor_window_.is_valid() || !peak_window_.is_valid()) {
        init_status_ = status::StatusNoMem;
        return;
    }

#ifdef ROC_TARGET_PROMETHEUS
    auto registry = metrics::prometheus_registry();

    const metrics::MetricsScope& scope = config_.prometheus.scope;
    const prometheus::Labels labels = metrics::scope_labels(scope);

    // Fixed bounds for the event duration and gap histograms. Durations
    // of interest span pause lengths (milliseconds to seconds); gaps
    // span event rates from several per second to one per many minutes.
    const metrics::HistogramConfig duration_buckets(40, 1 * core::Millisecond,
                                                    10 * core::Second);
    const metrics::HistogramConfig gap_buckets(40, 100 * core::Millisecond,
                                               1000 * core::Second);

    deviation_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "delay_deviation_seconds"))
             .Help("Per-packet arrival delay deviation in seconds: delay"
                   " level minus the exponential-mean baseline, clamped at"
                   " zero")
             .Register(*registry)
             .Add(labels,
                  metrics::generate_histogram_buckets(
                      config_.prometheus.delay_deviation));

    event_height_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "delay_event_height_seconds"))
             .Help("Peak deviation of one delay event, in seconds. An event"
                   " opens when the deviation exceeds the open threshold"
                   " (6 bulk standard deviations by default) and closes below"
                   " the close threshold (3 by default)")
             .Register(*registry)
             .Add(labels,
                  metrics::generate_histogram_buckets(config_.prometheus.event_height));

    event_duration_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "delay_event_duration_seconds"))
             .Help("Stream time one delay event spent above the close"
                   " threshold, in seconds")
             .Register(*registry)
             .Add(labels, metrics::generate_histogram_buckets(duration_buckets));

    event_gap_histogram_ =
        &prometheus::BuildHistogram()
             .Name(metrics::scope_metric_name(scope, "delay_event_gap_seconds"))
             .Help("Stream time between the close of one delay event and the"
                   " open of the next, in seconds")
             .Register(*registry)
             .Add(labels, metrics::generate_histogram_buckets(gap_buckets));

    baseline_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "delay_baseline_seconds"))
             .Help("Exponential mean of the arrival delay level in seconds."
                   " The constant part is arbitrary; only changes are"
                   " meaningful")
             .Register(*registry)
             .Add(labels);

    floor_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "delay_floor_seconds"))
             .Help("Rolling minimum of the arrival delay level in seconds")
             .Register(*registry)
             .Add(labels);

    bulk_stddev_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "delay_bulk_stddev_seconds"))
             .Help("Exponential standard deviation of the signed arrival"
                   " delay deviation, in seconds; updated outside events"
                   " only")
             .Register(*registry)
             .Add(labels);

    deviation_max_gauge_ =
        &prometheus::BuildGauge()
             .Name(metrics::scope_metric_name(scope, "delay_deviation_max_seconds"))
             .Help("Rolling maximum of the arrival delay deviation over the"
                   " peak window, in seconds")
             .Register(*registry)
             .Add(labels);

    event_counter_ =
        &prometheus::BuildCounter()
             .Name(metrics::scope_metric_name(scope, "delay_event_total"))
             .Help("Total delay events detected by the sigma-scaled"
                   " hysteresis detector")
             .Register(*registry)
             .Add(labels);
#endif

    init_status_ = status::StatusOK;
}

status::StatusCode ArrivalDelayMeter::init_status() const {
    return init_status_;
}

const ArrivalDelayMetrics& ArrivalDelayMeter::metrics() const {
    return metrics_;
}

void ArrivalDelayMeter::update_delay(core::nanoseconds_t level,
                                     core::nanoseconds_t duration) {
    roc_panic_if(init_status_ != status::StatusOK);

    if (duration < 0) {
        duration = 0;
    }

    stream_clock_ += duration;

    // Deviation against the baseline BEFORE this packet enters it, so
    // the deviation keeps its full size instead of being shrunk by its
    // own contribution to the mean. The signed deviation carries both
    // sides of the bulk noise and feeds the variance; the clamped
    // deviation is the margin-consuming observable for the histogram,
    // the peak window and the event detector.
    const bool has_baseline = baseline_.has();
    double signed_dev = 0;
    core::nanoseconds_t deviation = 0;
    if (has_baseline) {
        // Rounded to whole nanoseconds, the quantum of the level
        // itself: the sub-nanosecond float residue of the exponential
        // mean is not measurement noise, and rounding it away keeps
        // sigma exactly zero on a stream with zero deviation.
        const double d = (double)level - baseline_.get();
        signed_dev = (double)(core::nanoseconds_t)(d >= 0 ? d + 0.5 : d - 0.5);
        if (signed_dev > 0) {
            deviation = (core::nanoseconds_t)signed_dev;
        }
    }

    baseline_.update(alpha_(duration, config_.baseline_tau), (double)level);

    floor_window_.add(level);
    peak_window_.add(deviation);

    // Event detector. The variance is frozen while an event is open,
    // and the opening packet itself is already event, not bulk, so the
    // detector runs before the variance update. Events need both the
    // warmup weight and a positive variance: with sigma = 0 the
    // thresholds are not defined.
    const double sigma = curr_sigma_();

    if (!event_open_ && variance_.weight() >= MinVarianceWeight && sigma > 0
        && (double)deviation > config_.event_open_sigma * sigma) {
        open_event_();
    }

    if (event_open_) {
        if ((double)deviation <= config_.event_close_sigma * sigma) {
            close_event_();
        } else {
            if (deviation > event_height_) {
                event_height_ = deviation;
            }
            event_duration_ += duration;
        }
    }

    if (!event_open_ && has_baseline) {
        variance_.update(alpha_(duration, config_.sigma_tau), signed_dev * signed_dev);
    }

    interval_dev_sum_ += deviation;
    if (interval_packets_ == 0 || deviation > interval_dev_max_) {
        interval_dev_max_ = deviation;
    }
    interval_packets_++;

    metrics_.curr_deviation = deviation;
    if (baseline_.has()) {
        const double baseline = baseline_.get();
        metrics_.baseline = (core::nanoseconds_t)(baseline >= 0 ? baseline + 0.5
                                                                : baseline - 0.5);
    } else {
        metrics_.baseline = -1;
    }
    metrics_.floor = floor_window_.mov_min();
    metrics_.bulk_stddev = (core::nanoseconds_t)(curr_sigma_() + 0.5);
    metrics_.deviation_max = peak_window_.mov_max();
    metrics_.event_active = event_open_;

#ifdef ROC_TARGET_PROMETHEUS
    deviation_histogram_->Observe((double)deviation / 1e9);
#endif
}

void ArrivalDelayMeter::take_interval_stats(ArrivalDelayIntervalStats& stats) {
    roc_panic_if(init_status_ != status::StatusOK);

#ifdef ROC_TARGET_PROMETHEUS
    // Gauge refresh at interval rate: gauges carry the current state,
    // so any rate at or above the scrape rate is equivalent, and the
    // per-packet path stays free of gauge stores. Skipped until the
    // baseline accumulated weight (the gauges reference it).
    if (baseline_.has()) {
        baseline_gauge_->Set((double)metrics_.baseline / 1e9);
        floor_gauge_->Set((double)metrics_.floor / 1e9);
        bulk_stddev_gauge_->Set((double)metrics_.bulk_stddev / 1e9);
        deviation_max_gauge_->Set((double)metrics_.deviation_max / 1e9);
    }
#endif

    if (interval_packets_ == 0) {
        stats = ArrivalDelayIntervalStats();
        return;
    }

    stats.deviation_mean = interval_dev_sum_ / (core::nanoseconds_t)interval_packets_;
    stats.deviation_max = interval_dev_max_;
    stats.event_count = interval_events_;

    interval_dev_sum_ = 0;
    interval_dev_max_ = 0;
    interval_packets_ = 0;
    interval_events_ = 0;
}

double ArrivalDelayMeter::alpha_(core::nanoseconds_t duration,
                                 core::nanoseconds_t tau) const {
    double a = (double)duration / (double)tau;
    if (a > 1) {
        a = 1;
    }
    return a;
}

double ArrivalDelayMeter::curr_sigma_() const {
    if (!variance_.has() || variance_.get() <= 0) {
        return 0;
    }
    return sqrt(variance_.get());
}

void ArrivalDelayMeter::open_event_() {
    event_open_ = true;
    event_height_ = 0;
    event_duration_ = 0;

    has_event_gap_ = has_prev_close_;
    if (has_prev_close_) {
        event_gap_ = stream_clock_ - prev_close_clock_;
    }
}

void ArrivalDelayMeter::close_event_() {
    event_open_ = false;
    prev_close_clock_ = stream_clock_;
    has_prev_close_ = true;

    interval_events_++;

    metrics_.event_count++;

#ifdef ROC_TARGET_PROMETHEUS
    event_height_histogram_->Observe((double)event_height_ / 1e9);
    event_duration_histogram_->Observe((double)event_duration_ / 1e9);
    if (has_event_gap_) {
        event_gap_histogram_->Observe((double)event_gap_ / 1e9);
    }
    event_counter_->Increment();
#endif
}

} // namespace audio
} // namespace roc
