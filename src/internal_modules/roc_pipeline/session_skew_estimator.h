/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_pipeline/session_skew_estimator.h
//! @brief Session skew estimator.

#ifndef ROC_PIPELINE_SESSION_SKEW_ESTIMATOR_H_
#define ROC_PIPELINE_SESSION_SKEW_ESTIMATOR_H_

#include "roc_core/noncopyable.h"
#include "roc_core/stddefs.h"
#include "roc_core/time.h"
#include "roc_packet/stream_snapshot.h"
#include "roc_metrics/prometheus.h"
#include "roc_pipeline/config.h"
#include "roc_stat/exp_avg.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#endif

namespace roc {
namespace pipeline {

//! Session skew estimator.
//!
//! Sits above the slots of a session sender and turns their receivers'
//! stream snapshots into cross-receiver statistics. Rows are keyed by
//! the grid point CTS (identical across receivers by the shared-clock
//! invariant of the session sender); a row finalizes when every
//! registered slot contributed, or late with whoever showed up.
//!
//! Sync statistics build on e2e latency: capture timestamp to projected
//! playback on the receiver clock, the span listeners hear, including
//! sender batching, network transit and sink projection. They rest on
//! the documented deployment assumption that all hosts' clocks are
//! NTP/chrony-synchronized to microsecond-level error; e2e latency
//! inherits the clock error, so under this assumption e2e differences
//! between receivers equal playout-timing differences.
//!
//! Per finalized row, with l_i = e2e latency of slot i at the common
//! position: playout offset of slot i = l_i - median(l), the pairwise
//! skew matrix, fleet cross-section statistics (mean, population
//! stddev, max-min spread), and an offset jump detector (offset-step
//! events with magnitude, duration and per-slot counts). A slot whose
//! snapshot carries no e2e value (no RTCP clock mapping yet) is absent
//! from that row's sync statistics.
//!
//! Queue depth (arrival-to-read span) measures buffer margin, not
//! playout timing; it feeds only the EWMA covariance/correlation matrix
//! between slots and the per-slot RMS, a transport diagnostic
//! (Prometheus cannot reconstruct sub-scrape correlation from gauges).
//!
//! Single-threaded: all calls run on the sender pipeline thread.
class SessionSkewEstimator : public core::NonCopyable<> {
public:
    //! Maximum slots.
    static const size_t MaxSlots = 16;

    //! Per-slot gauge identifiers; one table entry each, so a gauge
    //! cannot be registered without a write site by construction.
    enum SlotGauge {
        Gauge_Offset = 0,
        Gauge_Rms,
        Gauge_Warp,
        Gauge_TargetLatency,
        Gauge_SnapshotTimestamp,
        Gauge_JumpActive,
        Gauge_JumpMagnitude,
        Gauge_JumpDuration,
        NumSlotGauges
    };


    //! Per-slot statistics, updated at row rate.
    struct SlotStats {
        bool valid;             //!< Whether the slot contributed to a row yet.
        double offset;          //!< E2E playout offset vs fleet median, seconds.
        double rms;             //!< EWMA RMS of queue-depth fluctuations, seconds.
        double warp;            //!< Last reported warp (coeff - 1).
        double target_latency;  //!< Last reported target latency, seconds.
        bool jump_active;     //!< Whether a offset jump event is in progress.
        double jump_magnitude; //!< Peak |offset - baseline| of last event, seconds.
        double jump_duration; //!< Duration of last completed event, seconds.
        uint64_t jump_count;  //!< Total offset jump events.
        core::nanoseconds_t last_update; //!< Arrival time of last snapshot.

        SlotStats()
            : valid(false)
            , offset(0)
            , rms(0)
            , warp(0)
            , target_latency(0)
            , jump_active(false)
            , jump_magnitude(0)
            , jump_duration(0)
            , jump_count(0)
            , last_update(0) {
        }
    };

    //! Per-pair statistics.
    struct PairStats {
        bool valid;  //!< Whether the pair co-occurred in a row yet.
        double skew; //!< Last l_i - l_j (e2e), seconds; zero until both
                     //!< slots reported e2e in a common row.
        double cov;  //!< EWMA covariance of queue-depth fluctuations, seconds^2.
        double corr; //!< EWMA correlation coefficient, [-1; 1].

        PairStats()
            : valid(false)
            , skew(0)
            , cov(0)
            , corr(0) {
        }
    };

    //! Fleet-level statistics.
    //! mean/stddev/spread are cross-section statistics of the e2e
    //! latencies at one grid instant, over the slots that reported e2e:
    //! location, scale (population stddev, divide by N), and extremes
    //! (max-min).
    struct FleetStats {
        bool valid;         //!< Whether any row with two e2e slots finalized yet.
        double mean;        //!< Mean e2e latency across slots, seconds.
        double stddev;      //!< Population stddev of e2e across slots, seconds.
        double spread;      //!< Last max-min of e2e across slots, seconds.
        uint64_t full_rows;    //!< Rows finalized with all slots present.
        uint64_t partial_rows; //!< Rows finalized late with missing slots.
        uint64_t rejected;     //!< Snapshots rejected (grid delta, bad slot).

        FleetStats()
            : valid(false)
            , mean(0)
            , stddev(0)
            , spread(0)
            , full_rows(0)
            , partial_rows(0)
            , rejected(0) {
        }
    };

    //! Initialize.
    //! @p prometheus_config supplies the metrics port (zero disables all
    //! metric registration) and the spread histogram bounds.
    SessionSkewEstimator(const SessionSkewEstimatorConfig& config,
                         const metrics::PrometheusConfig& prometheus_config);

    //! Register a slot; returns slot index or -1 if the table is full.
    //! An empty or duplicate @p name gets a unique generated label, so
    //! metric series never alias between slots.
    ssize_t register_slot(const char* name);

    //! Unregister a slot: rows stop waiting for it, its accumulated
    //! statistics are cleared, and its metric series are removed.
    void unregister_slot(size_t slot_index);

    //! Push one snapshot from a slot.
    //! @p grid_cts is the grid point on the sender CTS timeline (as
    //! reconstructed by the caller from the slot's RTP mapping);
    //! @p grid_delta is the deviation of the reported position from the
    //! grid point; @p arrival_time is the local receive time of the report.
    void process_snapshot(size_t slot_index,
                          core::nanoseconds_t grid_cts,
                          core::nanoseconds_t grid_period,
                          const packet::StreamSnapshot& sample,
                          core::nanoseconds_t grid_delta,
                          core::nanoseconds_t arrival_time);

    //! Read per-slot statistics; false if the slot is invalid.
    bool slot_stats(size_t slot_index, SlotStats& stats) const;

    //! Read pair statistics; false if either slot is invalid.
    bool pair_stats(size_t slot_a, size_t slot_b, PairStats& stats) const;

    //! Read fleet statistics.
    void fleet_stats(FleetStats& stats) const;

private:
    enum { MaxRows = 16, MaxNameLen = 64 };

    struct Row {
        bool used;
        bool finalized;
        core::nanoseconds_t grid_cts;
        core::nanoseconds_t grid_period;
        uint32_t present_mask;
        packet::StreamSnapshot samples[MaxSlots];
    };

    struct Slot {
        bool used;
        char name[MaxNameLen];

        SlotStats stats;

        bool has_prev_offset;
        double prev_offset;

        // Mean of the slot's offset; the absolute-bound reference of
        // the jump detector.
        stat::ExpAvg offset_mean;

        // Mean of the raw queue depth: the centering base for the
        // covariance/correlation. No fleet reference enters that path,
        // so pair correlation is a pure two-slot measurement.
        stat::ExpAvg q_mean;

        // Jump state.
        double jump_baseline;
        core::nanoseconds_t jump_start_cts;
        core::nanoseconds_t jump_hold_ns;
        core::nanoseconds_t jump_end_cts;

#ifdef ROC_TARGET_PROMETHEUS
        prometheus::Gauge* gauges[NumSlotGauges];
        prometheus::Counter* jump_counter;
#endif
    };


    void register_slot_metrics_(size_t slot_index);
    void register_pair_metrics_(size_t slot_a, size_t slot_b);
    void remove_slot_metrics_(size_t slot_index);
    void clear_slot_state_(size_t slot_index);
    double pair_corr_(size_t slot_a, size_t slot_b) const;

    Row* find_or_create_row_(core::nanoseconds_t grid_cts,
                             core::nanoseconds_t grid_period);
    void finalize_ready_rows_();
    void finalize_row_(Row& row);
    void update_jump_(size_t slot_index, double offset, const Row& row);

    const SessionSkewEstimatorConfig config_;

    Slot slots_[MaxSlots];
    Row rows_[MaxRows];

    // Bit i set while slots_[i].used; kept in sync by register_slot()
    // and unregister_slot() so rows never scan the table.
    uint32_t used_mask_;

    // Offset jump thresholds in seconds, fixed at construction.
    double jump_step_sec_;
    double jump_abs_sec_;
    double jump_release_sec_;

    // One record per slot pair, upper triangle; the diagonal holds the
    // EWMA variance. skew/valid mirror the PairStats API; cov is the
    // one source for both the correlation denominator and the gauge.
    struct Pair {
        bool valid;
        double skew;
        stat::ExpAvg cov;
#ifdef ROC_TARGET_PROMETHEUS
        prometheus::Gauge* skew_gauge;
        prometheus::Gauge* corr_gauge;
        prometheus::Gauge* cov_gauge;
#endif
    };

    Pair pairs_[MaxSlots][MaxSlots];

    FleetStats fleet_;

    core::nanoseconds_t newest_grid_cts_;

    // False when the metrics port is zero: no series are registered and
    // no gauge writes happen.
    const bool metrics_enabled_;

#ifdef ROC_TARGET_PROMETHEUS
    prometheus::Family<prometheus::Gauge>* slot_gauge_families_[NumSlotGauges];
    prometheus::Family<prometheus::Counter>* jump_counter_family_;
    prometheus::Family<prometheus::Gauge>* pair_gauge_families_[3];

    prometheus::Gauge* fleet_mean_gauge_;
    prometheus::Histogram* fleet_mean_histogram_;
    prometheus::Gauge* stddev_gauge_;
    prometheus::Histogram* stddev_histogram_;
    prometheus::Gauge* spread_gauge_;
    prometheus::Histogram* spread_histogram_;
    prometheus::Counter* rows_full_counter_;
    prometheus::Counter* rows_partial_counter_;
    prometheus::Counter* rejected_counter_;
#endif
};

} // namespace pipeline
} // namespace roc

#endif // ROC_PIPELINE_SESSION_SKEW_ESTIMATOR_H_
