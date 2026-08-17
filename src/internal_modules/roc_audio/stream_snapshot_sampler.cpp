/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_audio/stream_snapshot_sampler.h"
#include "roc_core/log.h"
#include "roc_core/panic.h"

namespace roc {
namespace audio {

namespace {

// Beyond this many grid periods of discontinuity the sampler resyncs
// instead of emitting stale crossings (seek, mapping jump, restart).
const core::nanoseconds_t MaxLagPeriods = 2;

} // namespace

StreamSnapshotSampler::StreamSnapshotSampler(const SampleSpec& sample_spec,
                                             core::nanoseconds_t grid_period)
    : sample_spec_(sample_spec)
    , grid_period_(grid_period)
    , has_mapping_(false)
    , map_cts_(0)
    , map_rtp_(0)
    , synced_(false)
    , next_grid_cts_(0)
    , niq_accum_(0)
    , niq_accum_count_(0)
    , ring_size_(0)
    , ring_head_(0) {
    roc_panic_if_msg(grid_period < 0, "stream snapshot sampler: negative grid period");
}

bool StreamSnapshotSampler::is_enabled() const {
    return grid_period_ > 0;
}

core::nanoseconds_t StreamSnapshotSampler::grid_period() const {
    return grid_period_;
}

void StreamSnapshotSampler::update_mapping(core::nanoseconds_t capture_ts,
                                           packet::stream_timestamp_t stream_ts) {
    if (!is_enabled()) {
        return;
    }

    has_mapping_ = true;
    map_cts_ = capture_ts;
    map_rtp_ = stream_ts;
}

void StreamSnapshotSampler::process_read(packet::stream_timestamp_t position,
                                         core::nanoseconds_t niq_latency,
                                         core::nanoseconds_t e2e_latency,
                                         float freq_coeff,
                                         core::nanoseconds_t target_latency,
                                         core::nanoseconds_t local_time) {
    if (!is_enabled() || !has_mapping_) {
        return;
    }

    // Position on the sender CTS timeline (wrap-safe RTP delta).
    const core::nanoseconds_t cts_now = map_cts_
        + sample_spec_.stream_timestamp_delta_2_ns(
              packet::stream_timestamp_diff(position, map_rtp_));

    if (cts_now <= 0) {
        return;
    }

    if (!synced_) {
        resync_(cts_now);
    }

    if (cts_now < next_grid_cts_ - grid_period_ * (MaxLagPeriods + 1)
        || cts_now >= next_grid_cts_ + grid_period_ * MaxLagPeriods) {
        // Discontinuity (seek, mapping jump, long stall): don't emit
        // stale crossings, restart the grid cursor.
        roc_log(LogDebug,
                "stream snapshot sampler: grid discontinuity: cts_now=%lld next=%lld",
                (long long)cts_now, (long long)next_grid_cts_);
        resync_(cts_now);
    }

    if (niq_latency >= 0) {
        niq_accum_ += niq_latency;
        niq_accum_count_++;
    }

    while (cts_now >= next_grid_cts_) {
        emit_(position, niq_latency, e2e_latency, freq_coeff, target_latency,
              local_time);
        next_grid_cts_ += grid_period_;
    }
}

size_t StreamSnapshotSampler::get_snapshots(StreamSnapshot* snapshots,
                                            size_t max_snapshots) const {
    roc_panic_if(!snapshots);

    size_t n_out = 0;
    for (size_t n = 0; n < ring_size_ && n_out < max_snapshots; n++) {
        // Oldest first: ring_head_ points at the next overwrite slot,
        // which is the oldest entry once the ring is full.
        const size_t index = (ring_head_ + MaxSnapshots - ring_size_ + n) % MaxSnapshots;
        snapshots[n_out++] = ring_[index];
    }

    return n_out;
}

void StreamSnapshotSampler::emit_(packet::stream_timestamp_t position,
                                  core::nanoseconds_t niq_latency,
                                  core::nanoseconds_t e2e_latency,
                                  float freq_coeff,
                                  core::nanoseconds_t target_latency,
                                  core::nanoseconds_t local_time) {
    StreamSnapshot snap;

    snap.grid_index = (uint32_t)(next_grid_cts_ / grid_period_);
    snap.position = position;
    snap.niq_instant = niq_latency >= 0 ? niq_latency : -1;
    if (niq_accum_count_ > 0) {
        snap.niq_mean = niq_accum_ / (core::nanoseconds_t)niq_accum_count_;
    }
    snap.e2e_latency = e2e_latency >= 0 ? e2e_latency : -1;
    if (freq_coeff != 0) {
        snap.has_warp = true;
        const double ppb = (double)(freq_coeff - 1.f) * 1e9;
        snap.warp_ppb = (int32_t)(ppb >= 0 ? ppb + 0.5 : ppb - 0.5);
    }
    snap.target_latency = target_latency >= 0 ? target_latency : -1;
    snap.recv_local_time = local_time;

    ring_[ring_head_] = snap;
    ring_head_ = (ring_head_ + 1) % MaxSnapshots;
    if (ring_size_ < MaxSnapshots) {
        ring_size_++;
    }

    reset_accum_();
}

void StreamSnapshotSampler::resync_(core::nanoseconds_t cts_now) {
    synced_ = true;
    next_grid_cts_ = (cts_now / grid_period_ + 1) * grid_period_;
    reset_accum_();
}

void StreamSnapshotSampler::reset_accum_() {
    niq_accum_ = 0;
    niq_accum_count_ = 0;
}

} // namespace audio
} // namespace roc
