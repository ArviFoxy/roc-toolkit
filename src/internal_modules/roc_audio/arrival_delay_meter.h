/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_audio/arrival_delay_meter.h
//! @brief Arrival delay meter.

#ifndef ROC_AUDIO_ARRIVAL_DELAY_METER_H_
#define ROC_AUDIO_ARRIVAL_DELAY_METER_H_

#include "roc_core/iarena.h"
#include "roc_core/noncopyable.h"
#include "roc_core/stddefs.h"
#include "roc_core/time.h"
#include "roc_metrics/prometheus.h"
#include "roc_stat/exp_avg.h"
#include "roc_stat/mov_min_max.h"
#include "roc_status/status_code.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#endif

namespace roc {
namespace audio {

//! Arrival delay meter parameters.
struct ArrivalDelayMeterConfig {
    //! Time constant of the baseline exponential mean.
    //! @remarks
    //!  Matched to the absorption bandwidth of the latency controller:
    //!  the controller absorbs the low-pass component of the delay into
    //!  warp, so the deviation (delay minus baseline) is the part that
    //!  consumes buffer margin.
    core::nanoseconds_t baseline_tau;

    //! Time constant of the diffusion-scale exponential variance.
    core::nanoseconds_t sigma_tau;

    //! Event open threshold, in bulk standard deviations.
    //! @remarks
    //!  An event opens when the deviation exceeds this many sigma. For
    //!  light-tailed bulk noise 6 sigma gives a false-event rate of
    //!  about 1e-9 per packet, so a trigger is a pause, not noise.
    double event_open_sigma;

    //! Event close threshold, in bulk standard deviations.
    double event_close_sigma;

    //! Rolling-minimum window for the delay floor, in packets.
    //! @remarks
    //!  Baseline minus floor is the standing latency cost of the
    //!  jitter bulk. Diagnostic only; never a deviation reference.
    size_t floor_window;

    //! Rolling-maximum window for the deviation peak, in packets.
    //! Diagnostic only.
    size_t peak_window;

    //! Prometheus configuration for histogram bounds.
    metrics::PrometheusConfig prometheus;

    ArrivalDelayMeterConfig()
        : baseline_tau(3 * core::Second)
        , sigma_tau(60 * core::Second)
        , event_open_sigma(6.0)
        , event_close_sigma(3.0)
        , floor_window(3000)
        , peak_window(8192) {
    }
};

//! Arrival delay metrics.
struct ArrivalDelayMetrics {
    //! Deviation of the last packet: delay minus baseline, clamped at zero.
    core::nanoseconds_t curr_deviation;

    //! Exponential mean of the delay level; negative until the baseline
    //! accumulated weight. The constant part is arbitrary; only changes
    //! are meaningful.
    core::nanoseconds_t baseline;

    //! Rolling minimum of the delay level over the floor window.
    core::nanoseconds_t floor;

    //! Exponential standard deviation of the signed deviation outside
    //! events.
    core::nanoseconds_t bulk_stddev;

    //! Rolling maximum of the deviation over the peak window.
    core::nanoseconds_t deviation_max;

    //! Whether an event is open.
    bool event_active;

    //! Total closed events.
    uint64_t event_count;

    ArrivalDelayMetrics()
        : curr_deviation(0)
        , baseline(-1)
        , floor(0)
        , bulk_stddev(0)
        , deviation_max(0)
        , event_active(false)
        , event_count(0) {
    }
};

//! Arrival delay statistics accumulated over one snapshot interval.
struct ArrivalDelayIntervalStats {
    //! Mean deviation over the interval; negative if no packets arrived.
    core::nanoseconds_t deviation_mean;

    //! Maximum deviation over the interval; negative if no packets arrived.
    core::nanoseconds_t deviation_max;

    //! Events closed during the interval; negative if no packets arrived.
    int64_t event_count;

    ArrivalDelayIntervalStats()
        : deviation_mean(-1)
        , deviation_max(-1)
        , event_count(-1) {
    }
};

//! Arrival delay meter.
//!
//! Consumes the per-packet arrival delay LEVEL: packet enqueue time on
//! the local clock minus packet schedule time on the RTP timeline, up
//! to an additive constant. The constant (transit time plus clock
//! offset) cancels against the baseline, so the measurement is
//! clock-free: no clock synchronization between hosts is assumed.
//!
//! Decomposes the level into three additive terms and estimates each:
//!
//!  - baseline: slow drift (clock-rate difference, route shifts),
//!    removed by an exponential mean whose time constant matches the
//!    latency controller, so the remaining deviation is mean-zero on
//!    controller timescales;
//!  - diffusion: light-tailed bulk noise around the baseline, scale
//!    estimated by an exponential variance of the SIGNED deviation
//!    (level minus baseline, both signs). The symmetric bulk noise
//!    lives on both sides of the baseline, so the signed second moment
//!    is its variance; with that scale, the light-tail assumption
//!    makes the 6 sigma false-event rate about 1e-9 per packet. The
//!    nonnegative (clamped) deviation remains the observable for the
//!    histogram, the peak window and the event detector: only the
//!    positive side consumes margin;
//!  - pauses: delivery interruptions, detected as EVENTS by a
//!    hysteresis detector on the clamped deviation (open above
//!    event_open_sigma * sigma, close below event_close_sigma * sigma).
//!    Per event the meter records height (peak deviation = margin
//!    consumed), duration (stream time above the close threshold) and
//!    gap (stream time since the previous event closed). The variance
//!    is frozen while an event is open, so the bulk scale is not
//!    contaminated by the events it gates.
//!
//! No events are opened until the variance estimate has accumulated at
//! least half of its steady-state weight AND is positive: below that
//! the thresholds are not yet meaningful. The weight wait is about
//! 0.7 * sigma_tau of stream time; a stream whose deviation is exactly
//! zero (no noise at all) keeps the detector closed.
//!
//! Time is measured on the stream timeline: update_delay() receives the
//! stream-time advance attributed to each packet, which is also the
//! exponential-average step. A late or reordered packet carries zero
//! advance: its level still feeds the deviation statistics and the
//! event detector (a late packet genuinely consumed margin), but it
//! moves neither the averages nor the event clocks.
//!
//! NTP clock steps: chrony slews the local clock here, so the level
//! sees ramps, not steps; the baseline follows them within its time
//! constant, and the variance within sigma_tau.
class ArrivalDelayMeter : public core::NonCopyable<ArrivalDelayMeter> {
public:
    //! Initialize.
    ArrivalDelayMeter(const ArrivalDelayMeterConfig& config, core::IArena& arena);

    //! Check if the object was successfully constructed.
    status::StatusCode init_status() const;

    //! Get updated metrics.
    const ArrivalDelayMetrics& metrics() const;

    //! Update with the delay level of a newly received packet.
    //! @p level is the arrival delay level (arbitrary constant offset);
    //! @p duration is the stream-time advance attributed to the packet
    //! (zero for late, reordered or duplicate packets).
    void update_delay(core::nanoseconds_t level, core::nanoseconds_t duration);

    //! Read and reset the statistics of the current snapshot interval.
    //! Also refreshes the exported gauges: once per interval matches
    //! any scrape rate, so the per-packet path stays free of gauge
    //! stores.
    void take_interval_stats(ArrivalDelayIntervalStats& stats);

private:
    double alpha_(core::nanoseconds_t duration, core::nanoseconds_t tau) const;
    double curr_sigma_() const;
    void open_event_();
    void close_event_();

    const ArrivalDelayMeterConfig config_;

    ArrivalDelayMetrics metrics_;

    // Baseline: exponential mean of the level.
    stat::ExpAvg baseline_;

    // Diffusion scale: exponential mean of the squared signed
    // deviation. Advances only outside events.
    stat::ExpAvg variance_;

    // Diagnostic windows.
    stat::MovMinMax<core::nanoseconds_t> floor_window_;
    stat::MovMinMax<core::nanoseconds_t> peak_window_;

    // Stream-time clock: sum of packet durations. The time base of
    // event durations and gaps.
    core::nanoseconds_t stream_clock_;

    // Open-event state.
    bool event_open_;
    core::nanoseconds_t event_height_;
    core::nanoseconds_t event_duration_;
    bool has_event_gap_;
    core::nanoseconds_t event_gap_;

    // Stream clock at the last event close.
    bool has_prev_close_;
    core::nanoseconds_t prev_close_clock_;

    // Snapshot-interval accumulators.
    core::nanoseconds_t interval_dev_sum_;
    core::nanoseconds_t interval_dev_max_;
    size_t interval_packets_;
    int64_t interval_events_;

    status::StatusCode init_status_;

#ifdef ROC_TARGET_PROMETHEUS
    prometheus::Histogram* deviation_histogram_;
    prometheus::Histogram* event_height_histogram_;
    prometheus::Histogram* event_duration_histogram_;
    prometheus::Histogram* event_gap_histogram_;
    prometheus::Gauge* baseline_gauge_;
    prometheus::Gauge* floor_gauge_;
    prometheus::Gauge* bulk_stddev_gauge_;
    prometheus::Gauge* deviation_max_gauge_;
    prometheus::Counter* event_counter_;
#endif
};

} // namespace audio
} // namespace roc

#endif // ROC_AUDIO_ARRIVAL_DELAY_METER_H_
