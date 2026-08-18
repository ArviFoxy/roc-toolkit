/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_core/time.h"
#include "roc_metrics/prometheus.h"
#include "roc_packet/stream_snapshot.h"
#include "roc_pipeline/session_skew_estimator.h"

namespace roc {
namespace pipeline {

namespace {

typedef SessionSkewEstimator Est;

const core::nanoseconds_t Period = 500 * core::Millisecond;
const core::nanoseconds_t BaseCts = 1000 * core::Second;

// Deterministic LCG for synthetic noise.
struct Lcg {
    uint64_t state;
    explicit Lcg(uint64_t seed)
        : state(seed) {
    }
    // Uniform in [-1; 1].
    double next() {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return ((double)(state >> 11) / (double)(1ull << 53)) * 2 - 1;
    }
};

packet::StreamSnapshot make_sample(core::nanoseconds_t niq_mean,
                                   core::nanoseconds_t e2e = -1) {
    packet::StreamSnapshot sample;
    sample.niq_mean = niq_mean;
    sample.niq_instant = niq_mean;
    sample.e2e_latency = e2e;
    sample.has_warp = true;
    sample.warp_ppb = 1000;
    sample.target_latency = 32 * core::Millisecond;
    return sample;
}

// Feed one full row: every slot in `slots` gets its q value.
void feed_row(Est& est,
              const ssize_t* slots,
              const core::nanoseconds_t* q,
              size_t n_slots,
              size_t row_index,
              const core::nanoseconds_t* e2e = NULL) {
    for (size_t n = 0; n < n_slots; n++) {
        est.process_snapshot((size_t)slots[n],
                             BaseCts + (core::nanoseconds_t)row_index * Period, Period,
                             make_sample(q[n], e2e ? e2e[n] : -1), 0, BaseCts);
    }
}

} // namespace

TEST_GROUP(session_skew_estimator) {};

TEST(session_skew_estimator, zero_skew) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[3];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");
    CHECK(slots[0] >= 0 && slots[1] >= 0 && slots[2] >= 0);

    const core::nanoseconds_t q[3] = { 10 * core::Millisecond, 10 * core::Millisecond,
                                       10 * core::Millisecond };
    for (size_t row = 0; row < 5; row++) {
        feed_row(est, slots, q, 3, row);
    }

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK(fleet.valid);
    CHECK_EQUAL(5, fleet.full_rows);
    CHECK_EQUAL(0, fleet.partial_rows);
    CHECK_EQUAL(0, fleet.rejected);
    DOUBLES_EQUAL(0.010, fleet.mean, 1e-9);
    DOUBLES_EQUAL(0, fleet.stddev, 1e-9);
    DOUBLES_EQUAL(0, fleet.spread, 1e-9);

    for (size_t n = 0; n < 3; n++) {
        Est::SlotStats stats;
        CHECK(est.slot_stats((size_t)slots[n], stats));
        CHECK(stats.valid);
        DOUBLES_EQUAL(0, stats.offset, 1e-9);
        DOUBLES_EQUAL(1000e-9, stats.warp, 1e-12);
        DOUBLES_EQUAL(0.032, stats.target_latency, 1e-9);
        CHECK(!stats.jump_active);
        CHECK_EQUAL(0, stats.jump_count);
    }
}

TEST(session_skew_estimator, known_offsets_recovered) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[3];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");

    // q = {10, 12, 11} ms; median 11ms; offsets {-1, +1, 0} ms.
    const core::nanoseconds_t q[3] = { 10 * core::Millisecond, 12 * core::Millisecond,
                                       11 * core::Millisecond };
    feed_row(est, slots, q, 3, 0);

    Est::SlotStats stats;
    CHECK(est.slot_stats((size_t)slots[0], stats));
    DOUBLES_EQUAL(-0.001, stats.offset, 1e-9);
    CHECK(est.slot_stats((size_t)slots[1], stats));
    DOUBLES_EQUAL(0.001, stats.offset, 1e-9);
    CHECK(est.slot_stats((size_t)slots[2], stats));
    DOUBLES_EQUAL(0, stats.offset, 1e-9);

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    DOUBLES_EQUAL(0.011, fleet.mean, 1e-9);
    // Population stddev of {-1, +1, 0} ms around the mean.
    DOUBLES_EQUAL(0.001 * sqrt(2.0 / 3.0), fleet.stddev, 1e-9);
    DOUBLES_EQUAL(0.002, fleet.spread, 1e-9);

    // Pairwise skew, both orientations.
    Est::PairStats pair;
    CHECK(est.pair_stats((size_t)slots[0], (size_t)slots[1], pair));
    CHECK(pair.valid);
    DOUBLES_EQUAL(-0.002, pair.skew, 1e-9);
    CHECK(est.pair_stats((size_t)slots[1], (size_t)slots[0], pair));
    DOUBLES_EQUAL(0.002, pair.skew, 1e-9);
}

TEST(session_skew_estimator, covariance_recovery) {
    SessionSkewEstimatorConfig config;
    // Short tau so the EWMA converges within the test.
    config.stats_tau = 10 * core::Second;
    Est est(config, metrics::PrometheusConfig());

    // Five slots: a and b share a dominant fluctuation (1ms), c/d/e are
    // quiet (0.1ms own noise). The shared component must be a MINORITY
    // signal: with a majority moving together the fleet median would
    // track it and correctly cancel it as common mode. Here the median
    // is one of the quiet slots, so the shared component survives into
    // the a/b offsets and their correlation, while (a, c) stays low.
    ssize_t slots[5];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");
    slots[3] = est.register_slot("d");
    slots[4] = est.register_slot("e");

    Lcg shared(1), own_a(2), own_b(3), own_c(4), own_d(5), own_e(6);

    for (size_t row = 0; row < 400; row++) {
        const double common = shared.next();
        const core::nanoseconds_t q[5] = {
            10 * core::Millisecond
                + (core::nanoseconds_t)(common * 1e6 + own_a.next() * 1e5),
            10 * core::Millisecond
                + (core::nanoseconds_t)(common * 1e6 + own_b.next() * 1e5),
            10 * core::Millisecond + (core::nanoseconds_t)(own_c.next() * 1e5),
            10 * core::Millisecond + (core::nanoseconds_t)(own_d.next() * 1e5),
            10 * core::Millisecond + (core::nanoseconds_t)(own_e.next() * 1e5),
        };
        feed_row(est, slots, q, 5, row);
    }

    Est::PairStats pair_ab, pair_ac;
    CHECK(est.pair_stats((size_t)slots[0], (size_t)slots[1], pair_ab));
    CHECK(est.pair_stats((size_t)slots[0], (size_t)slots[2], pair_ac));

    CHECK(pair_ab.valid && pair_ac.valid);
    CHECK(pair_ab.corr > 0.8);
    CHECK(pair_ab.corr > pair_ac.corr + 0.4);
    CHECK(pair_ab.cov > 0);

    // RMS reflects the fluctuation scale: ~0.6ms RMS for the 1ms-amplitude
    // shared component, well under the 10ms bound.
    Est::SlotStats stats;
    CHECK(est.slot_stats((size_t)slots[0], stats));
    CHECK(stats.rms > 0);
    CHECK(stats.rms < 0.01);
}

TEST(session_skew_estimator, missing_slot_partial_row) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[3];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");

    // Slot c never reports for row 0; rows 1..3 are complete. Row 0
    // finalizes late (3 periods behind) with 2 slots.
    const core::nanoseconds_t q2[2] = { 10 * core::Millisecond, 12 * core::Millisecond };
    feed_row(est, slots, q2, 2, 0);

    const core::nanoseconds_t q3[3] = { 10 * core::Millisecond, 12 * core::Millisecond,
                                        11 * core::Millisecond };
    for (size_t row = 1; row <= 3; row++) {
        feed_row(est, slots, q3, 3, row);
    }

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(3, fleet.full_rows);
    CHECK_EQUAL(1, fleet.partial_rows);
}

TEST(session_skew_estimator, fleet_stats_over_present_slots) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[3];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");

    // Slot c never reports: every row finalizes late with N=2, and the
    // cross-section statistics use the two present slots only.
    const core::nanoseconds_t q2[2] = { 10 * core::Millisecond, 12 * core::Millisecond };
    for (size_t row = 0; row <= 4; row++) {
        feed_row(est, slots, q2, 2, row);
    }

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(0, fleet.full_rows);
    CHECK(fleet.partial_rows >= 1);
    DOUBLES_EQUAL(0.011, fleet.mean, 1e-9);
    // Population stddev of {10, 12} ms: N=2 divisor, not N-1.
    DOUBLES_EQUAL(0.001, fleet.stddev, 1e-9);
    DOUBLES_EQUAL(0.002, fleet.spread, 1e-9);
}

TEST(session_skew_estimator, idempotent_duplicates) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[2];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");

    const core::nanoseconds_t q[2] = { 10 * core::Millisecond, 12 * core::Millisecond };

    // Same row delivered three times (RTCP re-sends the batch).
    feed_row(est, slots, q, 2, 0);
    feed_row(est, slots, q, 2, 0);
    feed_row(est, slots, q, 2, 0);

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(1, fleet.full_rows);
    CHECK_EQUAL(0, fleet.partial_rows);

    // Out-of-order old row long gone: ignored, not re-finalized.
    for (size_t row = 1; row <= 10; row++) {
        feed_row(est, slots, q, 2, row);
    }
    feed_row(est, slots, q, 2, 0);

    est.fleet_stats(fleet);
    CHECK_EQUAL(11, fleet.full_rows);
}

TEST(session_skew_estimator, grid_delta_gate) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slot_a = est.register_slot("a");
    ssize_t slot_b = est.register_slot("b");

    est.process_snapshot((size_t)slot_a, BaseCts, Period,
                         make_sample(10 * core::Millisecond),
                         30 * core::Millisecond /* > max_grid_delta */, BaseCts);

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(1, fleet.rejected);

    // The row was not created for the rejected snapshot.
    est.process_snapshot((size_t)slot_b, BaseCts, Period,
                         make_sample(10 * core::Millisecond), 0, BaseCts);
    est.fleet_stats(fleet);
    CHECK_EQUAL(0, fleet.full_rows);
}

TEST(session_skew_estimator, offset_jump_detection) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[2];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");

    // Steady baseline.
    const core::nanoseconds_t q_base[2] = { 10 * core::Millisecond,
                                            10 * core::Millisecond };
    size_t row = 0;
    for (; row < 10; row++) {
        feed_row(est, slots, q_base, 2, row);
    }

    Est::SlotStats stats;
    CHECK(est.slot_stats((size_t)slots[0], stats));
    CHECK(!stats.jump_active);
    CHECK_EQUAL(0, stats.jump_count);

    // Slot a jumps +5ms (offset jumps by +2.5ms, above the 2ms step
    // threshold: median moves too).
    const core::nanoseconds_t q_jump[2] = { 15 * core::Millisecond,
                                            10 * core::Millisecond };
    for (size_t n = 0; n < 4; n++, row++) {
        feed_row(est, slots, q_jump, 2, row);
    }

    CHECK(est.slot_stats((size_t)slots[0], stats));
    CHECK(stats.jump_active);
    CHECK_EQUAL(1, stats.jump_count);
    CHECK(stats.jump_magnitude > 0.002);

    // Recovery: back to baseline and hold for > jump_hold (2s = 4 rows).
    for (size_t n = 0; n < 6; n++, row++) {
        feed_row(est, slots, q_base, 2, row);
    }

    CHECK(est.slot_stats((size_t)slots[0], stats));
    CHECK(!stats.jump_active);
    CHECK_EQUAL(1, stats.jump_count);

    // The quiet slot never offset jumped.
    CHECK(est.slot_stats((size_t)slots[1], stats));
    CHECK_EQUAL(1, stats.jump_count); // median shift makes b jump too
}

TEST(session_skew_estimator, e2e_disagreement) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[3];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");

    // Equal queue depths (clock-free offsets zero), but slot a's e2e is
    // shifted +3ms by NTP error: mapping_error isolates it.
    const core::nanoseconds_t q[3] = { 10 * core::Millisecond, 10 * core::Millisecond,
                                       10 * core::Millisecond };
    const core::nanoseconds_t e2e[3] = { 35 * core::Millisecond, 32 * core::Millisecond,
                                         32 * core::Millisecond };
    feed_row(est, slots, q, 3, 0, e2e);

    Est::SlotStats stats;
    CHECK(est.slot_stats((size_t)slots[0], stats));
    DOUBLES_EQUAL(0, stats.offset, 1e-9);
    DOUBLES_EQUAL(0.003, stats.offset_e2e, 1e-9);
    DOUBLES_EQUAL(-0.003, stats.mapping_error, 1e-9);

    CHECK(est.slot_stats((size_t)slots[1], stats));
    DOUBLES_EQUAL(0, stats.offset_e2e, 1e-9);
    DOUBLES_EQUAL(0, stats.mapping_error, 1e-9);
}

TEST(session_skew_estimator, unregister_slot) {
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[3];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");
    slots[2] = est.register_slot("c");

    const core::nanoseconds_t q3[3] = { 10 * core::Millisecond, 12 * core::Millisecond,
                                        11 * core::Millisecond };
    feed_row(est, slots, q3, 3, 0);

    // Slot c leaves: rows now complete with 2 slots, immediately.
    est.unregister_slot((size_t)slots[2]);

    const core::nanoseconds_t q2[2] = { 10 * core::Millisecond, 12 * core::Millisecond };
    feed_row(est, slots, q2, 2, 1);

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(2, fleet.full_rows);

    Est::SlotStats stats;
    CHECK(!est.slot_stats((size_t)slots[2], stats));
}

TEST(session_skew_estimator, future_grid_gate) {
    // A grid point far in the future of the arrival clock is rejected
    // and must not advance the newest-row cursor: later normal rows
    // still finalize.
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[2];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");

    est.process_snapshot((size_t)slots[0], BaseCts + 3600 * core::Second, Period,
                         make_sample(10 * core::Millisecond), 0, BaseCts);

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(1, fleet.rejected);

    const core::nanoseconds_t q[2] = { 10 * core::Millisecond, 12 * core::Millisecond };
    for (size_t row = 0; row < 3; row++) {
        feed_row(est, slots, q, 2, row);
    }

    est.fleet_stats(fleet);
    CHECK_EQUAL(3, fleet.full_rows);
}

TEST(session_skew_estimator, mean_required) {
    // A snapshot without an interval mean is rejected: rows never mix
    // interval means with instantaneous values.
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slot_a = est.register_slot("a");
    est.register_slot("b");

    packet::StreamSnapshot sample = make_sample(-1);
    sample.niq_instant = 10 * core::Millisecond;
    est.process_snapshot((size_t)slot_a, BaseCts, Period, sample, 0, BaseCts);

    Est::FleetStats fleet;
    est.fleet_stats(fleet);
    CHECK_EQUAL(1, fleet.rejected);
    CHECK_EQUAL(0, fleet.full_rows);
}

TEST(session_skew_estimator, freshness_only_on_accept) {
    // Rejected snapshots must not refresh the slot's last-update time.
    SessionSkewEstimatorConfig config;
    Est est(config, metrics::PrometheusConfig());

    ssize_t slot_a = est.register_slot("a");
    est.register_slot("b");

    est.process_snapshot((size_t)slot_a, BaseCts, Period,
                         make_sample(10 * core::Millisecond),
                         30 * core::Millisecond /* rejected */, BaseCts);

    Est::SlotStats stats;
    CHECK(est.slot_stats((size_t)slot_a, stats));
    LONGLONGS_EQUAL(0, stats.last_update);

    est.process_snapshot((size_t)slot_a, BaseCts, Period,
                         make_sample(10 * core::Millisecond), 0, BaseCts);

    CHECK(est.slot_stats((size_t)slot_a, stats));
    LONGLONGS_EQUAL(BaseCts, stats.last_update);
}

TEST(session_skew_estimator, jump_closes_on_level_shift) {
    // A permanent level shift must not keep the event active forever.
    SessionSkewEstimatorConfig config;
    config.jump_max_duration = 3 * core::Second; // 6 rows at 500ms
    Est est(config, metrics::PrometheusConfig());

    ssize_t slots[2];
    slots[0] = est.register_slot("a");
    slots[1] = est.register_slot("b");

    const core::nanoseconds_t q_base[2] = { 10 * core::Millisecond,
                                            10 * core::Millisecond };
    size_t row = 0;
    for (; row < 10; row++) {
        feed_row(est, slots, q_base, 2, row);
    }

    // Slot a shifts +5ms and STAYS there.
    const core::nanoseconds_t q_shift[2] = { 15 * core::Millisecond,
                                             10 * core::Millisecond };
    for (size_t n = 0; n < 3; n++, row++) {
        feed_row(est, slots, q_shift, 2, row);
    }

    Est::SlotStats stats;
    CHECK(est.slot_stats((size_t)slots[0], stats));
    CHECK(stats.jump_active);

    // Hold the new level past jump_max_duration.
    for (size_t n = 0; n < 8; n++, row++) {
        feed_row(est, slots, q_shift, 2, row);
    }

    CHECK(est.slot_stats((size_t)slots[0], stats));
    CHECK(!stats.jump_active);
    DOUBLES_EQUAL(3.0, stats.jump_duration, 1e-9);
}

} // namespace pipeline
} // namespace roc
