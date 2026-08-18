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

// Beyond this many grid periods (or read steps, whichever is larger)
// of discontinuity the sampler resyncs instead of emitting stale
// crossings (seek, mapping jump, restart). The read-step term matters
// when the grid period is smaller than the read step: a normal read
// then advances by several grid periods and must not look like a
// discontinuity.
const int64_t MaxLagPeriods = 2;

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
    , next_grid_rtp_(0)
    , has_prev_cts_(false)
    , prev_cts_(0)
    , last_step_(0)
    , candidate_step_(0)
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

    if (synced_) {
        update_rtp_cursor_();
    }
}

void StreamSnapshotSampler::process_read(packet::stream_timestamp_t position,
                                         core::nanoseconds_t niq_latency,
                                         core::nanoseconds_t e2e_latency,
                                         double freq_coeff,
                                         core::nanoseconds_t target_latency) {
    if (!is_enabled() || !has_mapping_) {
        return;
    }

    if (niq_latency >= 0) {
        niq_accum_ += niq_latency;
        niq_accum_count_++;
    }

    // Fast path: no grid point was crossed. A single wrap-safe integer
    // compare; the nanosecond conversion below runs only on crossings
    // and on the first read after sync.
    if (synced_ && packet::stream_timestamp_lt(position, next_grid_rtp_)) {
        return;
    }

    // Position on the sender CTS timeline (wrap-safe RTP delta).
    const core::nanoseconds_t cts_now = map_cts_
        + sample_spec_.stream_timestamp_delta_2_ns(
              packet::stream_timestamp_diff(position, map_rtp_));

    if (cts_now <= 0) {
        return;
    }

    // Discontinuity bound: larger of the grid scale and the LAST
    // ACCEPTED read step. The step term keeps small grids working (a
    // normal read then advances by several grid periods); using the
    // last accepted step, not the current one, keeps a genuine jump
    // detectable.
    core::nanoseconds_t max_jump = grid_period_ * MaxLagPeriods;
    if (last_step_ * MaxLagPeriods > max_jump) {
        max_jump = last_step_ * MaxLagPeriods;
    }

    // Learn the read cadence: promote a step only when two consecutive
    // steps are similar. A one-off jump (seek, stall) never repeats,
    // so it cannot widen the discontinuity bound; a real cadence does.
    const core::nanoseconds_t step = has_prev_cts_ ? cts_now - prev_cts_ : 0;
    has_prev_cts_ = true;
    prev_cts_ = cts_now;
    if (step > 0) {
        if (candidate_step_ > 0 && step < candidate_step_ * 2
            && candidate_step_ < step * 2) {
            last_step_ = step;
        }
        candidate_step_ = step;
    }

    if (!synced_) {
        resync_(cts_now);
    } else if (cts_now < next_grid_cts_ - max_jump - grid_period_
               || cts_now >= next_grid_cts_ + max_jump) {
        // Discontinuity (seek, mapping jump, long stall): don't emit
        // stale crossings, restart the grid cursor.
        roc_log(LogDebug,
                "stream snapshot sampler: grid discontinuity: cts_now=%lld next=%lld",
                (long long)cts_now, (long long)next_grid_cts_);
        resync_(cts_now);
    }

    // Emit at most one ring's worth of crossings per read; skip the
    // rest (the ring would overwrite them anyway).
    const int64_t pending =
        cts_now < next_grid_cts_ ? 0 : (cts_now - next_grid_cts_) / grid_period_ + 1;
    if (pending > (int64_t)MaxSnapshots) {
        next_grid_cts_ += (pending - (int64_t)MaxSnapshots) * grid_period_;
    }

    while (cts_now >= next_grid_cts_) {
        emit_(niq_latency, e2e_latency, freq_coeff, target_latency);
        next_grid_cts_ += grid_period_;
    }

    if (ring_size_ > 0) {
        // Accumulation restarts after the read that crossed; crossings
        // within one read share the same interval mean.
        reset_accum_();
    }

    update_rtp_cursor_();
}

size_t StreamSnapshotSampler::get_snapshots(packet::StreamSnapshot* snapshots,
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

void StreamSnapshotSampler::emit_(core::nanoseconds_t niq_latency,
                                  core::nanoseconds_t e2e_latency,
                                  double freq_coeff,
                                  core::nanoseconds_t target_latency) {
    packet::StreamSnapshot snap;

    snap.grid_index = (uint32_t)(next_grid_cts_ / grid_period_);
    // Report the RTP position of the GRID POINT itself, not of the
    // read that crossed it. The sender validates this position against
    // the grid, so it must not carry the read-cadence overshoot.
    snap.position = map_rtp_
        + (packet::stream_timestamp_t)sample_spec_.ns_2_stream_timestamp_delta(
              next_grid_cts_ - map_cts_);
    snap.niq_instant = niq_latency >= 0 ? niq_latency : -1;
    if (niq_accum_count_ > 0) {
        snap.niq_mean = niq_accum_ / (core::nanoseconds_t)niq_accum_count_;
    }
    snap.e2e_latency = e2e_latency >= 0 ? e2e_latency : -1;
    if (freq_coeff != 0) {
        snap.has_warp = true;
        const double ppb = (freq_coeff - 1.0) * 1e9;
        snap.warp_ppb = (int32_t)(ppb >= 0 ? ppb + 0.5 : ppb - 0.5);
    }
    snap.target_latency = target_latency >= 0 ? target_latency : -1;

    ring_[ring_head_] = snap;
    ring_head_ = (ring_head_ + 1) % MaxSnapshots;
    if (ring_size_ < MaxSnapshots) {
        ring_size_++;
    }
}

void StreamSnapshotSampler::resync_(core::nanoseconds_t cts_now) {
    synced_ = true;
    next_grid_cts_ = (cts_now / grid_period_ + 1) * grid_period_;
    reset_accum_();
    update_rtp_cursor_();
}

void StreamSnapshotSampler::update_rtp_cursor_() {
    next_grid_rtp_ = map_rtp_
        + (packet::stream_timestamp_t)sample_spec_.ns_2_stream_timestamp_delta(
              next_grid_cts_ - map_cts_);
}

void StreamSnapshotSampler::reset_accum_() {
    niq_accum_ = 0;
    niq_accum_count_ = 0;
}

} // namespace audio
} // namespace roc
