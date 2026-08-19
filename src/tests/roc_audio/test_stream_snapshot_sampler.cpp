/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_audio/stream_snapshot_sampler.h"
#include "roc_core/heap_arena.h"
#include "roc_core/time.h"

namespace roc {
namespace audio {

namespace {

enum { SampleRate = 48000, ChMask = 0x1 };

const SampleSpec sample_spec(
    SampleRate, PcmSubformat_Raw, ChanLayout_Surround, ChanOrder_Smpte, ChMask);

const core::nanoseconds_t GridPeriod = 500 * core::Millisecond;

// Stream timestamps per grid period.
const packet::stream_timestamp_t GridSamples = SampleRate / 2;

// One "read" worth of samples (5ms).
const packet::stream_timestamp_t ReadSamples = SampleRate / 200;

// Mapping anchor: CTS is aligned so grid math is easy to reason about.
const core::nanoseconds_t MapCts = 1000 * core::Second;
const packet::stream_timestamp_t MapRtp = 100000;

void feed_reads(StreamSnapshotSampler& sampler,
                packet::stream_timestamp_t from_pos,
                packet::stream_timestamp_t to_pos,
                core::nanoseconds_t niq) {
    // Wrap-safe iteration: positions may cross the 32-bit boundary.
    const packet::stream_timestamp_diff_t total =
        packet::stream_timestamp_diff(to_pos, from_pos);
    for (packet::stream_timestamp_diff_t off = 0; off < total; off += ReadSamples) {
        sampler.process_read(from_pos + (packet::stream_timestamp_t)off, niq, -1, 0,
                             -1);
    }
}

} // namespace

TEST_GROUP(stream_snapshot_sampler) {};

TEST(stream_snapshot_sampler, disabled) {
    StreamSnapshotSampler sampler(sample_spec, 0, NULL);

    CHECK(!sampler.is_enabled());

    sampler.update_mapping(MapCts, MapRtp);
    feed_reads(sampler, MapRtp, MapRtp + GridSamples * 4, core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(0, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));
}

TEST(stream_snapshot_sampler, no_mapping_inert) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);

    CHECK(sampler.is_enabled());

    feed_reads(sampler, MapRtp, MapRtp + GridSamples * 4, core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(0, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));
}

TEST(stream_snapshot_sampler, basic_crossings) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // Two full grid periods of reads with constant niq.
    feed_reads(sampler, MapRtp, MapRtp + GridSamples * 2 + ReadSamples,
               2 * core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(2, n);

    // MapCts = 1000s, grid 500ms: first crossing is grid index 2001.
    const uint32_t base_index = (uint32_t)(MapCts / GridPeriod);
    CHECK_EQUAL(base_index + 1, snaps[0].grid_index);
    CHECK_EQUAL(base_index + 2, snaps[1].grid_index);

    // Consecutive indices; positions are grid-aligned, so exactly one
    // grid period apart (the read-cadence overshoot must not leak into
    // the reported position).
    CHECK_EQUAL(snaps[0].grid_index + 1, snaps[1].grid_index);
    const packet::stream_timestamp_diff_t pos_delta =
        packet::stream_timestamp_diff(snaps[1].position, snaps[0].position);
    LONGS_EQUAL((packet::stream_timestamp_diff_t)GridSamples, pos_delta);

    // Constant niq: instant == mean == fed value.
    LONGLONGS_EQUAL(2 * core::Millisecond, snaps[1].niq_instant);
    LONGLONGS_EQUAL(2 * core::Millisecond, snaps[1].niq_mean);

    // Unavailable fields carry sentinels.
    CHECK(snaps[1].e2e_latency < 0);
    CHECK(!snaps[1].has_warp);
    CHECK(snaps[1].target_latency < 0);

    // Non-destructive read: same result again.
    packet::StreamSnapshot again[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(n, sampler.get_snapshots(again, StreamSnapshotSampler::MaxSnapshots));
    CHECK_EQUAL(snaps[0].grid_index, again[0].grid_index);
    CHECK_EQUAL(snaps[1].grid_index, again[1].grid_index);
}

TEST(stream_snapshot_sampler, niq_mean_averages_interval) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // First partial interval reaches the first crossing.
    feed_reads(sampler, MapRtp, MapRtp + GridSamples + ReadSamples,
               4 * core::Millisecond);

    // Second interval: half the reads at 2ms, half at 6ms -> mean 4ms,
    // instant (last value) 6ms.
    packet::stream_timestamp_t pos = MapRtp + GridSamples + ReadSamples;
    const packet::stream_timestamp_t half = MapRtp + GridSamples + GridSamples / 2;
    for (; pos < half; pos += ReadSamples) {
        sampler.process_read(pos, 2 * core::Millisecond, -1, 0, -1);
    }
    for (; pos < MapRtp + GridSamples * 2 + ReadSamples; pos += ReadSamples) {
        sampler.process_read(pos, 6 * core::Millisecond, -1, 0, -1);
    }

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(2, n);

    LONGLONGS_EQUAL(6 * core::Millisecond, snaps[1].niq_instant);
    CHECK(snaps[1].niq_mean > 3 * core::Millisecond + core::Millisecond / 2);
    CHECK(snaps[1].niq_mean < 4 * core::Millisecond + core::Millisecond / 2);
}

TEST(stream_snapshot_sampler, ring_overwrites_oldest) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // Seven crossings; ring keeps the last 4, oldest first.
    feed_reads(sampler, MapRtp, MapRtp + GridSamples * 7 + ReadSamples,
               core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(StreamSnapshotSampler::MaxSnapshots, n);

    const uint32_t base_index = (uint32_t)(MapCts / GridPeriod);
    for (size_t i = 0; i < n; i++) {
        CHECK_EQUAL(base_index + 4 + i, snaps[i].grid_index);
    }
}

TEST(stream_snapshot_sampler, metric_capture) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    for (packet::stream_timestamp_t pos = MapRtp;
         pos < MapRtp + GridSamples + ReadSamples; pos += ReadSamples) {
        sampler.process_read(pos, 3 * core::Millisecond, 32 * core::Millisecond,
                             1.000010, 32 * core::Millisecond);
    }

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(1, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));

    LONGLONGS_EQUAL(32 * core::Millisecond, snaps[0].e2e_latency);
    CHECK(snaps[0].has_warp);
    CHECK_EQUAL(10000, snaps[0].warp_ppb);
    LONGLONGS_EQUAL(32 * core::Millisecond, snaps[0].target_latency);
}

TEST(stream_snapshot_sampler, negative_warp) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    for (packet::stream_timestamp_t pos = MapRtp;
         pos < MapRtp + GridSamples + ReadSamples; pos += ReadSamples) {
        sampler.process_read(pos, core::Millisecond, -1, 0.999990, -1);
    }

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(1, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));

    CHECK(snaps[0].has_warp);
    CHECK_EQUAL(-10000, snaps[0].warp_ppb);
}

TEST(stream_snapshot_sampler, discontinuity_resync) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    feed_reads(sampler, MapRtp, MapRtp + GridSamples + ReadSamples,
               core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(1, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));
    const uint32_t first_index = snaps[0].grid_index;

    // Jump 10 grid periods ahead (seek / long stall): must resync,
    // not emit 10 stale crossings.
    const packet::stream_timestamp_t jump_pos = MapRtp + GridSamples * 11;
    feed_reads(sampler, jump_pos, jump_pos + GridSamples + ReadSamples,
               core::Millisecond);

    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(2, n);
    CHECK_EQUAL(first_index, snaps[0].grid_index);
    // Post-jump crossing lands near the jump target, not at first+1.
    CHECK(snaps[1].grid_index >= first_index + 11);
}

TEST(stream_snapshot_sampler, mapping_update_small_shift) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    feed_reads(sampler, MapRtp, MapRtp + GridSamples + ReadSamples,
               core::Millisecond);

    // Refreshed SR mapping, shifted by 1ms (normal mapping noise):
    // sampling continues, no resync, indices stay consecutive.
    sampler.update_mapping(MapCts + GridSamples * 2 * core::Nanosecond * 0 + core::Millisecond,
                           MapRtp);

    feed_reads(sampler, MapRtp + GridSamples + ReadSamples,
               MapRtp + GridSamples * 2 + ReadSamples, core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(2, n);
    CHECK_EQUAL(snaps[0].grid_index + 1, snaps[1].grid_index);
}

TEST(stream_snapshot_sampler, rtp_wraparound) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);

    // Anchor just below the 32-bit wrap; positions cross it.
    const packet::stream_timestamp_t wrap_rtp = (packet::stream_timestamp_t)-24000;
    sampler.update_mapping(MapCts, wrap_rtp);

    feed_reads(sampler, wrap_rtp, wrap_rtp + GridSamples * 2 + ReadSamples,
               core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(2, n);
    CHECK_EQUAL(snaps[0].grid_index + 1, snaps[1].grid_index);
}

TEST(stream_snapshot_sampler, grid_smaller_than_read) {
    // Grid period far below the read step: every read crosses several
    // grid points. The discontinuity guard must not fire, and the ring
    // fills with the newest crossings.
    const core::nanoseconds_t SmallGrid = 3 * core::Millisecond;
    StreamSnapshotSampler sampler(sample_spec, SmallGrid, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // 10ms reads for 200ms of stream.
    const packet::stream_timestamp_t BigRead = SampleRate / 100;
    for (packet::stream_timestamp_t off = 0; off < SampleRate / 5; off += BigRead) {
        sampler.process_read(MapRtp + off, core::Millisecond, -1, 0, -1);
    }

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK_EQUAL(StreamSnapshotSampler::MaxSnapshots, n);

    // Newest snapshots carry consecutive grid indices and valid means.
    for (size_t i = 1; i < n; i++) {
        CHECK_EQUAL(snaps[i - 1].grid_index + 1, snaps[i].grid_index);
    }
    for (size_t i = 0; i < n; i++) {
        CHECK(snaps[i].niq_mean >= 0);
    }
}

TEST(stream_snapshot_sampler, process_read_returns_emitted_count) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // No crossing yet.
    CHECK_EQUAL(0, sampler.process_read(MapRtp, core::Millisecond, -1, 0, -1));

    // A read past the first grid point emits one snapshot.
    CHECK_EQUAL(1,
                sampler.process_read(MapRtp + GridSamples + ReadSamples,
                                     core::Millisecond, -1, 0, -1));

    // A read jumping two grid periods emits two.
    CHECK_EQUAL(2,
                sampler.process_read(MapRtp + GridSamples * 3 + ReadSamples,
                                     core::Millisecond, -1, 0, -1));
}

TEST(stream_snapshot_sampler, drain_accumulation_and_reset) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // No interval closed yet: sentinel.
    LONGLONGS_EQUAL(-1, sampler.last_interval_drain());

    // Interval with queue depth varying between 2ms and 6ms, mean 4ms:
    // drain = mean - min = 2ms.
    packet::stream_timestamp_t pos = MapRtp;
    size_t i = 0;
    for (; pos < MapRtp + GridSamples + ReadSamples; pos += ReadSamples, i++) {
        const core::nanoseconds_t niq =
            i % 2 == 0 ? 2 * core::Millisecond : 6 * core::Millisecond;
        sampler.process_read(pos, niq, -1, 0, -1);
    }

    CHECK(sampler.last_interval_drain() >= core::Millisecond);
    CHECK(sampler.last_interval_drain() <= 3 * core::Millisecond);

    // Next interval with constant depth: accumulation was reset, so
    // the drain of the new interval is zero, not a mixture.
    for (; pos < MapRtp + GridSamples * 2 + ReadSamples; pos += ReadSamples) {
        sampler.process_read(pos, 5 * core::Millisecond, -1, 0, -1);
    }
    LONGLONGS_EQUAL(0, sampler.last_interval_drain());
}

TEST(stream_snapshot_sampler, drain_starved_interval_no_value) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    // Healthy interval first.
    packet::stream_timestamp_t pos = MapRtp;
    for (; pos < MapRtp + GridSamples + ReadSamples; pos += ReadSamples) {
        sampler.process_read(pos, 5 * core::Millisecond, -1, 0, -1);
    }
    CHECK(sampler.last_interval_drain() >= 0);

    // An interval whose reads carry no queue depth closes with no
    // drain value: the healthy interval's value is not republished.
    for (; pos < MapRtp + GridSamples * 2 + ReadSamples; pos += ReadSamples) {
        sampler.process_read(pos, -1, -1, 0, -1);
    }
    LONGLONGS_EQUAL(-1, sampler.last_interval_drain());
}

TEST(stream_snapshot_sampler, drain_needs_two_readings) {
    // Grid period below the read step: each interval sees at most one
    // queue reading, and one reading makes mean minus min identically
    // zero. Such intervals yield no drain value.
    const core::nanoseconds_t SmallGrid = 3 * core::Millisecond;
    StreamSnapshotSampler sampler(sample_spec, SmallGrid, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    const packet::stream_timestamp_t BigRead = SampleRate / 100;
    for (packet::stream_timestamp_t off = 0; off < SampleRate / 5; off += BigRead) {
        sampler.process_read(MapRtp + off, core::Millisecond, -1, 0, -1);
    }
    LONGLONGS_EQUAL(-1, sampler.last_interval_drain());
}

TEST(stream_snapshot_sampler, delay_stats_stamped_and_reset) {
    core::HeapArena arena;
    ArrivalDelayMeterConfig meter_config;
    ArrivalDelayMeter meter(meter_config, arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    StreamSnapshotSampler sampler(sample_spec, GridPeriod, &meter);
    sampler.update_mapping(MapCts, MapRtp);

    // Packet path: constant 2ms deviation charged into the meter.
    // (The meter baseline starts empty, so the first update pins it
    // and later updates deviate from it.)
    meter.update_delay(0, core::Millisecond);
    for (size_t i = 0; i < 10; i++) {
        meter.update_delay(2 * core::Millisecond, 0);
    }

    // Frame path: one grid crossing pulls the interval statistics.
    feed_reads(sampler, MapRtp, MapRtp + GridSamples + ReadSamples,
               core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(1, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));

    CHECK(snaps[0].deviation_mean > core::Millisecond);
    CHECK(snaps[0].deviation_max >= 2 * core::Millisecond - 100 * core::Microsecond);
    LONGLONGS_EQUAL(0, snaps[0].event_count);

    // The pull reset the meter interval: a crossing with no packets in
    // between carries sentinels.
    feed_reads(sampler, MapRtp + GridSamples + ReadSamples,
               MapRtp + GridSamples * 2 + ReadSamples, core::Millisecond);
    CHECK_EQUAL(2, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));
    LONGLONGS_EQUAL(-1, snaps[1].deviation_mean);
    LONGLONGS_EQUAL(-1, snaps[1].deviation_max);
    LONGLONGS_EQUAL(-1, snaps[1].event_count);
}

TEST(stream_snapshot_sampler, delay_stats_first_snapshot_of_read_only) {
    core::HeapArena arena;
    ArrivalDelayMeterConfig meter_config;
    ArrivalDelayMeter meter(meter_config, arena);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    const core::nanoseconds_t SmallGrid = 100 * core::Millisecond;
    StreamSnapshotSampler sampler(sample_spec, SmallGrid, &meter);
    sampler.update_mapping(MapCts, MapRtp);

    meter.update_delay(0, core::Millisecond);
    meter.update_delay(3 * core::Millisecond, 0);

    sampler.process_read(MapRtp, core::Millisecond, -1, 0, -1);
    // One read jumping two grid periods: the delay statistics are
    // extensive over the one closed interval, so only the first
    // snapshot carries them and the rest carry sentinels (a replicated
    // event count would be counted per row at the sender).
    sampler.process_read(MapRtp + SampleRate / 5 + ReadSamples, core::Millisecond, -1,
                         0, -1);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK(n >= 2);
    CHECK(snaps[0].deviation_mean >= 0);
    CHECK(snaps[0].deviation_max >= 0);
    LONGLONGS_EQUAL(0, snaps[0].event_count);
    for (size_t i = 1; i < n; i++) {
        LONGLONGS_EQUAL(-1, snaps[i].deviation_mean);
        LONGLONGS_EQUAL(-1, snaps[i].deviation_max);
        LONGLONGS_EQUAL(-1, snaps[i].event_count);
    }
}

TEST(stream_snapshot_sampler, null_meter_sentinels) {
    StreamSnapshotSampler sampler(sample_spec, GridPeriod, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    feed_reads(sampler, MapRtp, MapRtp + GridSamples + ReadSamples,
               core::Millisecond);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    CHECK_EQUAL(1, sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots));
    LONGLONGS_EQUAL(-1, snaps[0].deviation_mean);
    LONGLONGS_EQUAL(-1, snaps[0].deviation_max);
    LONGLONGS_EQUAL(-1, snaps[0].event_count);
}

TEST(stream_snapshot_sampler, shared_mean_on_multi_crossing) {
    // Two grid points crossed by one read: both snapshots carry the
    // same interval mean; neither is unavailable.
    const core::nanoseconds_t SmallGrid = 100 * core::Millisecond;
    StreamSnapshotSampler sampler(sample_spec, SmallGrid, NULL);
    sampler.update_mapping(MapCts, MapRtp);

    sampler.process_read(MapRtp, 2 * core::Millisecond, -1, 0, -1);
    // One read jumping two grid periods forward.
    sampler.process_read(MapRtp + SampleRate / 5 + ReadSamples,
                         2 * core::Millisecond, -1, 0, -1);

    packet::StreamSnapshot snaps[StreamSnapshotSampler::MaxSnapshots];
    const size_t n = sampler.get_snapshots(snaps, StreamSnapshotSampler::MaxSnapshots);
    CHECK(n >= 2);
    for (size_t i = 0; i < n; i++) {
        LONGLONGS_EQUAL(2 * core::Millisecond, snaps[i].niq_mean);
    }
}

} // namespace audio
} // namespace roc
