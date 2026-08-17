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

namespace roc {
namespace pipeline {

//! Session skew estimator configuration.
struct SessionSkewEstimatorConfig {
    //! Reject snapshots whose position deviates from the grid point by
    //! more than this (indicates CTS mapping breakage).
    core::nanoseconds_t max_grid_delta;

    //! Time constant of the EWMA statistics (means, covariance).
    core::nanoseconds_t stats_tau;

    //! Time constant of the slow common-mode baseline.
    core::nanoseconds_t common_mode_tau;

    //! Flinch trigger: offset step between consecutive rows.
    core::nanoseconds_t flinch_step;

    //! Flinch trigger: absolute offset bound.
    core::nanoseconds_t flinch_abs;

    //! Flinch release: offset must return within this band of the
    //! pre-event baseline...
    core::nanoseconds_t flinch_release_band;

    //! ...and stay there for this long.
    core::nanoseconds_t flinch_hold;

    //! Rows older than this many grid periods behind the newest row are
    //! finalized even if some slots are missing.
    size_t late_row_periods;

    SessionSkewEstimatorConfig()
        : max_grid_delta(5 * core::Millisecond)
        , stats_tau(60 * core::Second)
        , common_mode_tau(60 * core::Second)
        , flinch_step(2 * core::Millisecond)
        , flinch_abs(10 * core::Millisecond)
        , flinch_release_band(500 * core::Microsecond)
        , flinch_hold(2 * core::Second)
        , late_row_periods(3) {
    }
};

//! Session skew estimator.
//!
//! Sits above the slots of a session sender and turns their receivers'
//! stream snapshots into cross-receiver statistics. Rows are keyed by
//! the grid point CTS (identical across receivers by the shared-clock
//! invariant of the session sender); a row finalizes when every
//! registered slot contributed, or late with whoever showed up.
//!
//! Per finalized row, with q_i = interval-mean queue depth of slot i at
//! the common position: playout offset of slot i = q_i - median(q), the
//! clock-free skew (common emission means receiver i plays the position
//! at arrival + q_i; no wall clocks involved). The e2e-based offset is
//! computed the same way from the receivers' e2e estimates and inherits
//! their NTP error; the difference of the two per slot IS the
//! differential clock-mapping error. Note the clock-free offset
//! measures the decode point: constant per-receiver device buffering
//! appears only in the e2e view.
//!
//! Also maintained at row rate: the full pairwise skew matrix and an
//! EWMA covariance/correlation matrix of offset fluctuations (Prometheus
//! cannot reconstruct sub-scrape correlation from gauges), fleet spread
//! and slow common-mode baseline, per-slot RMS, and a flinch detector
//! (offset-step events with magnitude, duration and per-slot counts).
//!
//! Single-threaded: all calls run on the sender pipeline thread.
class SessionSkewEstimator : public core::NonCopyable<> {
public:
    //! Maximum slots.
    static const size_t MaxSlots = 16;

    //! One slot's contribution to a grid row.
    struct SlotSample {
        core::nanoseconds_t niq_instant; //!< Queue depth at crossing; -1 unavail.
        core::nanoseconds_t niq_mean;    //!< Interval-mean queue depth; -1 unavail.
        core::nanoseconds_t e2e_latency; //!< E2E estimate; -1 unavail.
        bool has_warp;                   //!< Whether warp_ppb is valid.
        int32_t warp_ppb;                //!< Warp in parts per billion.
        core::nanoseconds_t target_latency;  //!< Target latency; -1 unavail.
        core::nanoseconds_t recv_local_time; //!< Receiver clock; 0 unavail.

        SlotSample()
            : niq_instant(-1)
            , niq_mean(-1)
            , e2e_latency(-1)
            , has_warp(false)
            , warp_ppb(0)
            , target_latency(-1)
            , recv_local_time(0) {
        }
    };

    //! Per-slot statistics, updated at row rate.
    struct SlotStats {
        bool valid;             //!< Whether the slot contributed to a row yet.
        double offset;          //!< Clock-free playout offset vs fleet median, seconds.
        double offset_e2e;      //!< E2E-based offset vs fleet median, seconds.
        double mapping_error;   //!< offset - offset_e2e, seconds.
        double rms;             //!< EWMA RMS of offset fluctuations, seconds.
        double warp;            //!< Last reported warp (coeff - 1).
        double target_latency;  //!< Last reported target latency, seconds.
        bool flinch_active;     //!< Whether a flinch event is in progress.
        double flinch_magnitude; //!< Peak |offset - baseline| of last event, seconds.
        double flinch_duration; //!< Duration of last completed event, seconds.
        uint64_t flinch_count;  //!< Total flinch events.
        core::nanoseconds_t last_update; //!< Arrival time of last snapshot.

        SlotStats()
            : valid(false)
            , offset(0)
            , offset_e2e(0)
            , mapping_error(0)
            , rms(0)
            , warp(0)
            , target_latency(0)
            , flinch_active(false)
            , flinch_magnitude(0)
            , flinch_duration(0)
            , flinch_count(0)
            , last_update(0) {
        }
    };

    //! Per-pair statistics.
    struct PairStats {
        bool valid;  //!< Whether the pair co-occurred in a row yet.
        double skew; //!< Last q_i - q_j, seconds.
        double cov;  //!< EWMA covariance of offset fluctuations, seconds^2.
        double corr; //!< EWMA correlation coefficient, [-1; 1].

        PairStats()
            : valid(false)
            , skew(0)
            , cov(0)
            , corr(0) {
        }
    };

    //! Fleet-level statistics.
    struct FleetStats {
        bool valid;         //!< Whether any row finalized yet.
        double spread;      //!< Last max-min of q across slots, seconds.
        double common_mode; //!< Fleet mean minus slow baseline, seconds.
        uint64_t full_rows;    //!< Rows finalized with all slots present.
        uint64_t partial_rows; //!< Rows finalized late with missing slots.
        uint64_t rejected;     //!< Snapshots rejected (grid delta, bad slot).

        FleetStats()
            : valid(false)
            , spread(0)
            , common_mode(0)
            , full_rows(0)
            , partial_rows(0)
            , rejected(0) {
        }
    };

    explicit SessionSkewEstimator(const SessionSkewEstimatorConfig& config);

    //! Register a slot; returns slot index or -1 if the table is full.
    ssize_t register_slot(const char* name);

    //! Unregister a slot: rows stop waiting for it.
    void unregister_slot(size_t slot_index);

    //! Get slot name.
    const char* slot_name(size_t slot_index) const;

    //! Get number of registered slot table entries (indices may be sparse).
    size_t num_slots() const;

    //! Push one snapshot from a slot.
    //! @p grid_cts is the grid point on the sender CTS timeline (as
    //! reconstructed by the caller from the slot's RTP mapping);
    //! @p grid_delta is the deviation of the reported position from the
    //! grid point; @p arrival_time is the local receive time of the report.
    void process_snapshot(size_t slot_index,
                          core::nanoseconds_t grid_cts,
                          core::nanoseconds_t grid_period,
                          const SlotSample& sample,
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
        SlotSample samples[MaxSlots];
    };

    struct Slot {
        bool used;
        char name[MaxNameLen];

        SlotStats stats;

        bool has_prev_offset;
        double prev_offset;
        double ewma_mean;
        bool has_ewma;

        // Flinch state.
        double flinch_baseline;
        core::nanoseconds_t flinch_start_cts;
        core::nanoseconds_t flinch_hold_ns;
    };

    Row* find_or_create_row_(core::nanoseconds_t grid_cts,
                             core::nanoseconds_t grid_period);
    void finalize_ready_rows_();
    void finalize_row_(Row& row);
    void update_flinch_(size_t slot_index, double offset, const Row& row);

    const SessionSkewEstimatorConfig config_;

    Slot slots_[MaxSlots];
    Row rows_[MaxRows];

    // EWMA cross-products of centered offsets, upper triangle including
    // the diagonal (variances).
    double cov_[MaxSlots][MaxSlots];
    bool cov_valid_[MaxSlots][MaxSlots];

    PairStats pair_stats_[MaxSlots][MaxSlots];

    FleetStats fleet_;
    double common_mode_baseline_;
    bool has_common_mode_baseline_;

    core::nanoseconds_t newest_grid_cts_;
};

} // namespace pipeline
} // namespace roc

#endif // ROC_PIPELINE_SESSION_SKEW_ESTIMATOR_H_
