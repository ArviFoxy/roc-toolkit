/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_audio/stream_snapshot_sampler.h
//! @brief Stream snapshot sampler.

#ifndef ROC_AUDIO_STREAM_SNAPSHOT_SAMPLER_H_
#define ROC_AUDIO_STREAM_SNAPSHOT_SAMPLER_H_

#include "roc_audio/sample_spec.h"
#include "roc_core/noncopyable.h"
#include "roc_core/stddefs.h"
#include "roc_core/time.h"
#include "roc_packet/capture_timestamp_mapping.h"
#include "roc_packet/stream_snapshot.h"
#include "roc_packet/units.h"

namespace roc {
namespace audio {

//! Stream snapshot sampler.
//!
//! Samples receiver-side telemetry on a grid defined on the SENDER CTS
//! timeline: grid point k sits at CTS = k * grid period. Because the
//! session sender stamps identical CTS across all its streams, every
//! receiver of the session samples the same instants with zero
//! coordination, each in its own stream's RTP timestamp domain.
//!
//! The CTS<->RTP mapping is pushed in from the RTCP SR (the same pair
//! that feeds rtp::TimestampInjector). The mapping is used only to
//! LABEL positions with grid indices; its (NTP-dependent) error shifts
//! which sample is considered the grid point, a second-order effect,
//! and never enters the reported queue-depth values themselves.
//!
//! Snapshots land in a small ring, newest last, read non-destructively:
//! the wire format is idempotent by grid index, so entries are re-sent
//! until they age out of the ring.
//!
//! Single-threaded: process_read(), update_mapping() and
//! get_snapshots() all run on the receiver pipeline thread.
class StreamSnapshotSampler : public core::NonCopyable<> {
public:
    //! Maximum snapshots kept (and reported per RTCP report).
    static const size_t MaxSnapshots = packet::MaxStreamSnapshots;

    //! Initialize.
    //! @p grid_period is the grid step on the sender CTS timeline;
    //! zero disables the sampler entirely.
    StreamSnapshotSampler(const SampleSpec& sample_spec,
                          core::nanoseconds_t grid_period);

    //! Check if sampling is enabled (non-zero grid period).
    bool is_enabled() const;

    //! Get grid period.
    core::nanoseconds_t grid_period() const;

    //! Update CTS<->RTP mapping: @p capture_ts corresponds to @p stream_ts.
    void update_mapping(core::nanoseconds_t capture_ts,
                        packet::stream_timestamp_t stream_ts);

    //! Feed one read.
    //! @p position is the stream position of the read (absolute RTP
    //! timestamp of the next sample to be decoded); @p niq_latency is
    //! the current queue depth (negative if unavailable); @p e2e_latency
    //! and @p target_latency are negative if unavailable; @p freq_coeff
    //! is zero if not yet computed.
    void process_read(packet::stream_timestamp_t position,
                      core::nanoseconds_t niq_latency,
                      core::nanoseconds_t e2e_latency,
                      double freq_coeff,
                      core::nanoseconds_t target_latency);

    //! Read up to @p max_snapshots snapshots, oldest first, newest last.
    //! Non-destructive; returns the number of snapshots copied.
    size_t get_snapshots(packet::StreamSnapshot* snapshots, size_t max_snapshots) const;

private:
    void emit_(core::nanoseconds_t niq_latency,
               core::nanoseconds_t e2e_latency,
               double freq_coeff,
               core::nanoseconds_t target_latency);
    void resync_(core::nanoseconds_t cts_now);
    void update_rtp_cursor_();
    void reset_accum_();

    const SampleSpec sample_spec_;
    const core::nanoseconds_t grid_period_;

    packet::CaptureTimestampMapping mapping_;

    bool synced_;
    core::nanoseconds_t next_grid_cts_;
    packet::stream_timestamp_t next_grid_rtp_;
    bool has_prev_cts_;
    core::nanoseconds_t prev_cts_;
    core::nanoseconds_t last_step_;
    core::nanoseconds_t candidate_step_;

    // niq accumulation over the current grid interval.
    core::nanoseconds_t niq_accum_;
    size_t niq_accum_count_;

    packet::StreamSnapshot ring_[MaxSnapshots];
    size_t ring_size_;
    size_t ring_head_;
};

} // namespace audio
} // namespace roc

#endif // ROC_AUDIO_STREAM_SNAPSHOT_SAMPLER_H_
