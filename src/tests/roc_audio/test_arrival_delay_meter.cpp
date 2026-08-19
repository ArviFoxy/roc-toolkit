/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_audio/arrival_delay_meter.h"
#include "roc_core/heap_arena.h"

namespace roc {
namespace audio {

namespace {

core::HeapArena arena;

// One packet per millisecond of stream time.
const core::nanoseconds_t Period = core::Millisecond;

// Small time constants so warmup and convergence fit in a test.
ArrivalDelayMeterConfig make_config() {
    ArrivalDelayMeterConfig config;
    config.baseline_tau = 100 * core::Millisecond;
    config.sigma_tau = core::Second;
    return config;
}

// Feed n packets of constant level.
void feed_flat(ArrivalDelayMeter& meter, core::nanoseconds_t level, size_t n) {
    for (size_t i = 0; i < n; i++) {
        meter.update_delay(level, Period);
    }
}

// Feed n packets alternating level - amp, level + amp: bulk noise with
// a deterministic, symmetric shape.
void feed_noise(ArrivalDelayMeter& meter,
                core::nanoseconds_t level,
                core::nanoseconds_t amp,
                size_t n) {
    for (size_t i = 0; i < n; i++) {
        const core::nanoseconds_t v = i % 2 == 0 ? level - amp : level + amp;
        meter.update_delay(v, Period);
    }
}

// Feed one pause episode: packets scheduled during the pause all arrive
// when it ends, so the level ramps down from `depth` to the base level.
// Returns the stream time during which the detector reported an open
// event (the externally observable event duration).
core::nanoseconds_t feed_pause(ArrivalDelayMeter& meter,
                               core::nanoseconds_t level,
                               core::nanoseconds_t depth) {
    core::nanoseconds_t active = 0;
    for (core::nanoseconds_t d = depth; d > 0; d -= Period) {
        meter.update_delay(level + d, Period);
        if (meter.metrics().event_active) {
            active += Period;
        }
    }
    return active;
}

// Absolute difference of two nanosecond values.
core::nanoseconds_t ns_abs(core::nanoseconds_t x) {
    return x >= 0 ? x : -x;
}

// Warmup: enough noise packets that the variance estimate passed its
// weight floor (0.7 * sigma_tau) and the baseline converged.
void warmup(ArrivalDelayMeter& meter, core::nanoseconds_t level) {
    feed_noise(meter, level, 20 * core::Microsecond, 1000);
}

} // namespace

TEST_GROUP(arrival_delay_meter) {};

TEST(arrival_delay_meter, init) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const ArrivalDelayMetrics& metrics = meter.metrics();
    CHECK(!metrics.event_active);
    UNSIGNED_LONGS_EQUAL(0, metrics.event_count);
}

TEST(arrival_delay_meter, baseline_tracks_mean_shift) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level1 = 5 * core::Millisecond;
    feed_flat(meter, level1, 1000);
    // 1000 packets = 10 time constants: converged.
    CHECK(ns_abs(meter.metrics().baseline - level1) < 100 * core::Microsecond);

    const core::nanoseconds_t level2 = 7 * core::Millisecond;
    feed_flat(meter, level2, 1000);
    CHECK(ns_abs(meter.metrics().baseline - level2) < 100 * core::Microsecond);

    // One time constant after a shift the baseline covered about
    // 1 - 1/e (63%) of the distance.
    ArrivalDelayMeter meter2(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter2.init_status());
    feed_flat(meter2, level1, 1000);
    feed_flat(meter2, level2, 100);
    const core::nanoseconds_t travelled = meter2.metrics().baseline - level1;
    const core::nanoseconds_t distance = level2 - level1;
    CHECK(travelled > distance / 2);
    CHECK(travelled < distance * 3 / 4);
}

TEST(arrival_delay_meter, floor_and_peak) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    // Floor sits at the lowest level seen in the window.
    CHECK(ns_abs(meter.metrics().floor - (level - 20 * core::Microsecond))
          < core::Microsecond);

    // A spike raises the deviation peak.
    meter.update_delay(level + 2 * core::Millisecond, Period);
    CHECK(meter.metrics().deviation_max > core::Millisecond);
}

TEST(arrival_delay_meter, pause_is_one_event_height_matches_duration) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);
    UNSIGNED_LONGS_EQUAL(0, meter.metrics().event_count);

    // Drop the warmup interval so the next pull covers only the pause.
    ArrivalDelayIntervalStats stats;
    meter.take_interval_stats(stats);

    const core::nanoseconds_t depth = 30 * core::Millisecond;
    const core::nanoseconds_t active = feed_pause(meter, level, depth);
    feed_noise(meter, level, 20 * core::Microsecond, 10);

    const ArrivalDelayMetrics& metrics = meter.metrics();
    UNSIGNED_LONGS_EQUAL(1, metrics.event_count);
    CHECK(!metrics.event_active);

    // Height = interval deviation max = pause depth = margin consumed
    // (baseline drift over the event is small against the depth).
    meter.take_interval_stats(stats);
    CHECK(ns_abs(stats.deviation_max - depth) < 5 * core::Millisecond);

    // The pause-shape prediction: the ramp keeps the detector open for
    // about as long as the peak deviation was high.
    CHECK(ns_abs(active - depth) < 10 * core::Millisecond);
}

TEST(arrival_delay_meter, two_pauses_two_events) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    // Two pauses separated by bulk close as two events, not one merged
    // event: no decluster constant exists.
    feed_pause(meter, level, 30 * core::Millisecond);
    UNSIGNED_LONGS_EQUAL(1, meter.metrics().event_count);
    feed_noise(meter, level, 20 * core::Microsecond, 200);
    UNSIGNED_LONGS_EQUAL(1, meter.metrics().event_count);
    feed_pause(meter, level, 30 * core::Millisecond);
    feed_noise(meter, level, 20 * core::Microsecond, 10);

    const ArrivalDelayMetrics& metrics = meter.metrics();
    UNSIGNED_LONGS_EQUAL(2, metrics.event_count);
    CHECK(!metrics.event_active);
}

TEST(arrival_delay_meter, sigma_frozen_during_event) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    const core::nanoseconds_t sigma_before = meter.metrics().bulk_stddev;
    CHECK(sigma_before > 0);

    // Hold the level high: the event stays open the whole time. If the
    // variance were not frozen, 500 packets of huge deviation would
    // multiply sigma.
    for (size_t i = 0; i < 500; i++) {
        meter.update_delay(level + 50 * core::Millisecond, Period);
        CHECK(meter.metrics().event_active);
        LONGS_EQUAL(sigma_before, meter.metrics().bulk_stddev);
    }

    // Back to bulk: the event closes and the variance resumes.
    feed_noise(meter, level, 40 * core::Microsecond, 2000);
    CHECK(!meter.metrics().event_active);
    CHECK(meter.metrics().bulk_stddev > sigma_before);
}

TEST(arrival_delay_meter, no_events_during_warmup) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;

    // 100 packets is well below the variance weight floor
    // (0.7 * sigma_tau = 700 packets here).
    feed_noise(meter, level, 20 * core::Microsecond, 100);

    meter.update_delay(level + 50 * core::Millisecond, Period);
    CHECK(!meter.metrics().event_active);
    UNSIGNED_LONGS_EQUAL(0, meter.metrics().event_count);

    // The same spike after warmup opens an event.
    warmup(meter, level);
    meter.update_delay(level + 50 * core::Millisecond, Period);
    CHECK(meter.metrics().event_active);
}

TEST(arrival_delay_meter, no_events_without_noise) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;

    // A stream with zero deviation keeps sigma at zero; the event
    // thresholds are then undefined, so the detector stays closed even
    // for a large spike, well past the warmup weight.
    feed_flat(meter, level, 2000);
    LONGS_EQUAL(0, meter.metrics().bulk_stddev);

    meter.update_delay(level + 50 * core::Millisecond, Period);
    CHECK(!meter.metrics().event_active);
    UNSIGNED_LONGS_EQUAL(0, meter.metrics().event_count);
}

TEST(arrival_delay_meter, downtrend_arms_without_storm) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    // A steadily falling level (clock-rate difference): the baseline
    // lags above it, so the clamped deviation is zero packet after
    // packet, while the signed deviation is a negative constant near
    // slope * baseline_tau. The variance arms with that scale.
    const core::nanoseconds_t start = 500 * core::Millisecond;
    const core::nanoseconds_t slope = 10 * core::Microsecond;
    const size_t n_packets = 3000;

    for (size_t i = 0; i < n_packets; i++) {
        meter.update_delay(start - (core::nanoseconds_t)i * slope, Period);
    }

    // No event storm: the falling level itself opens nothing.
    UNSIGNED_LONGS_EQUAL(0, meter.metrics().event_count);
    CHECK(!meter.metrics().event_active);

    // Sigma equals the baseline lag: slope per packet times the number
    // of packets in one baseline time constant.
    const core::nanoseconds_t lag = slope * (make_config().baseline_tau / Period);
    CHECK(meter.metrics().bulk_stddev > lag / 2);
    CHECK(meter.metrics().bulk_stddev < lag * 2);

    // The armed detector still opens on a genuine spike.
    const core::nanoseconds_t curr =
        start - (core::nanoseconds_t)n_packets * slope;
    meter.update_delay(curr + 50 * core::Millisecond, Period);
    CHECK(meter.metrics().event_active);
}

TEST(arrival_delay_meter, event_totals_on_close_only) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    ArrivalDelayIntervalStats stats;
    meter.take_interval_stats(stats);

    // While the event is open nothing is counted yet.
    meter.update_delay(level + 50 * core::Millisecond, Period);
    CHECK(meter.metrics().event_active);
    UNSIGNED_LONGS_EQUAL(0, meter.metrics().event_count);

    // The close increments the counter; the spike height shows up as
    // the interval deviation max.
    meter.update_delay(level, Period);
    CHECK(!meter.metrics().event_active);
    UNSIGNED_LONGS_EQUAL(1, meter.metrics().event_count);

    meter.take_interval_stats(stats);
    CHECK(stats.deviation_max > 45 * core::Millisecond);
    LONGS_EQUAL(1, stats.event_count);
}

TEST(arrival_delay_meter, zero_duration_moves_no_averages) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    const core::nanoseconds_t baseline_before = meter.metrics().baseline;
    const core::nanoseconds_t sigma_before = meter.metrics().bulk_stddev;

    // A late packet carries zero stream advance: its level feeds the
    // deviation statistics but moves neither average.
    meter.update_delay(level + 2 * core::Millisecond, 0);
    LONGS_EQUAL(baseline_before, meter.metrics().baseline);
    LONGS_EQUAL(sigma_before, meter.metrics().bulk_stddev);
    CHECK(meter.metrics().curr_deviation > core::Millisecond);
}

TEST(arrival_delay_meter, interval_stats_pull_reset_sentinels) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    // Empty interval: all sentinels.
    ArrivalDelayIntervalStats stats;
    meter.take_interval_stats(stats);
    LONGS_EQUAL(-1, stats.deviation_mean);
    LONGS_EQUAL(-1, stats.deviation_max);
    LONGS_EQUAL(-1, stats.event_count);

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    // Drop the warmup interval, then measure a known one.
    meter.take_interval_stats(stats);

    feed_flat(meter, level, 9);
    meter.update_delay(level + core::Millisecond, Period);

    meter.take_interval_stats(stats);
    CHECK(stats.deviation_mean >= 50 * core::Microsecond);
    CHECK(stats.deviation_mean <= 200 * core::Microsecond);
    CHECK(stats.deviation_max > 900 * core::Microsecond);
    LONGS_EQUAL(0, stats.event_count);

    // The pull reset the interval: an immediate second pull is empty.
    meter.take_interval_stats(stats);
    LONGS_EQUAL(-1, stats.deviation_mean);
    LONGS_EQUAL(-1, stats.deviation_max);
    LONGS_EQUAL(-1, stats.event_count);
}

TEST(arrival_delay_meter, interval_stats_count_event_closes) {
    ArrivalDelayMeter meter(make_config(), arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t level = 5 * core::Millisecond;
    warmup(meter, level);

    ArrivalDelayIntervalStats stats;
    meter.take_interval_stats(stats);

    // An event that opens in this interval but has not closed yet is
    // not counted.
    meter.update_delay(level + 30 * core::Millisecond, Period);
    CHECK(meter.metrics().event_active);
    meter.take_interval_stats(stats);
    LONGS_EQUAL(0, stats.event_count);

    // The close lands in the next interval.
    meter.update_delay(level, Period);
    meter.take_interval_stats(stats);
    LONGS_EQUAL(1, stats.event_count);
}

} // namespace audio
} // namespace roc
