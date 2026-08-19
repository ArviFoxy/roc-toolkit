/*
 * Copyright (c) 2015 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_packet/delayed_reader.h
//! @brief Delayed reader.

#ifndef ROC_PACKET_DELAYED_READER_H_
#define ROC_PACKET_DELAYED_READER_H_

#include "roc_audio/sample_spec.h"
#include "roc_core/noncopyable.h"
#include "roc_core/time.h"
#include "roc_packet/capture_timestamp_mapping.h"
#include "roc_packet/ireader.h"
#include "roc_packet/sorted_queue.h"
#include "roc_packet/units.h"

namespace roc {
namespace packet {

//! Delayed reader parameters.
struct DelayedReaderConfig {
    //! Delay to insert before the first packet.
    core::nanoseconds_t target_delay;

    //! If true, choose the start position from the wall clock: playback
    //! begins at the stream position captured target_delay before the
    //! local time, computed from the capture timestamp mapping pushed via
    //! update_mapping() and the local time pushed via update_local_time().
    //! @remarks
    //!  Assumes sender and receiver clocks are synchronized (e.g. NTP).
    //!  A mapping that is stale or disagrees with the queue contents by
    //!  more than cut_tolerance is discarded; a later mapping may still
    //!  align the start. When no usable mapping arrives, or the queue
    //!  doesn't cover the aligned position, within
    //!  start_alignment_timeout, the depth-based start applies.
    bool start_alignment;

    //! How long to wait for a usable mapping and for queue coverage of
    //! the aligned start position before falling back to the depth-based
    //! start.
    //! @remarks
    //!  Must be positive when start_alignment is enabled; the receiver
    //!  pipeline fills it from the deduced latency configuration.
    core::nanoseconds_t start_alignment_timeout;

    //! Maximum distance between the clock-derived cut and the cut implied
    //! by the queue depth before the mapping is distrusted and discarded.
    //! @remarks
    //!  Must be positive when start_alignment is enabled; the receiver
    //!  pipeline fills it from the latency tolerance, so an aligned start
    //!  is never chosen where the latency tuner would abort the session.
    core::nanoseconds_t cut_tolerance;

    DelayedReaderConfig()
        : target_delay(0)
        , start_alignment(false)
        , start_alignment_timeout(0)
        , cut_tolerance(0) {
    }
};

//! Delayed reader.
//!
//! Delays read of the first packet in stream for the configured duration.
//!
//! Assumes that packets arrive at constant rate, and pipeline performs read
//! from delayed reader at the same rate (in average).
//!
//! Operation is split into three stages:
//!
//!   1. Loading: reads packets from incoming queue and accumulates them in
//!      delay queue. Doesn't return packets to pipeline. This stage lasts
//!      until target delay is accumulated. By the end of this stage,
//!      incoming queue length is zero, delay queue length is target delay,
//!      and pipeline is ahead of the last packet in queue by target delay.
//!
//!   2. Unloading: returns packets from delay queue until it becomes empty.
//!      Doesn't read packets from incoming queue. By the end of this stage,
//!      incoming queue length is target delay, delay queue length is zero,
//!      and pipeline is ahead of the last packet in queue by target delay.
//!
//!   3. Forwarding: just forwards packets from incoming queue and doesn't
//!      use delay queue anymore. Incoming queue length remains equal to
//!      target delay, given that packets packets are arriving and read
//!      at the same rate.
//!
//! If start alignment is enabled, the Loading stage selects the first
//! packet from the wall clock instead of the queue depth: playback starts
//! at the position whose capture timestamp is target delay before the
//! local time, so that receivers sharing synchronized clocks and a common
//! target delay start at the same stream position. The head trim is the
//! same whole-packet trim as in the depth-based start, only the cut point
//! is clock-derived.
class DelayedReader : public IReader, public core::NonCopyable<> {
public:
    //! Initialize.
    //!
    //! @b Parameters
    //!  - @p reader is used to read packets from incoming queue
    //!  - @p config defines the delay to insert before first packet
    //!  - @p sample_spec is the specifications of incoming packets
    DelayedReader(IReader& reader,
                  const DelayedReaderConfig& config,
                  const audio::SampleSpec& sample_spec);

    //! Check if the object was successfully constructed.
    status::StatusCode init_status() const;

    //! Set mapping between capture timestamp and stream timestamp.
    //! The aligned start uses it to locate the start position; the latest
    //! mapping wins. Mappings with non-positive capture timestamp are
    //! ignored.
    void update_mapping(core::nanoseconds_t capture_ts, stream_timestamp_t rtp_ts);

    //! Set current time of the local clock.
    //! Unix time in nanoseconds, same clock domain as the capture
    //! timestamps passed to update_mapping(). Should be called regularly
    //! before reads; the aligned start evaluates its timeout and start
    //! position against the last pushed value.
    void update_local_time(core::nanoseconds_t local_now);

    //! Read packet.
    virtual ROC_NODISCARD status::StatusCode read(PacketPtr& packet, PacketReadMode mode);

private:
    enum StartMode {
        // Start position derived from the wall clock and the mapping.
        StartAligned,
        // Start position derived from the queue depth alone.
        StartDepth
    };

    status::StatusCode load_queue_();
    status::StatusCode aligned_start_();
    status::StatusCode trim_to_(stream_timestamp_t cut, size_t& n_dropped);
    void log_start_(const char* mode,
                    const char* reason,
                    stream_timestamp_diff_t cut_offset,
                    stream_timestamp_t init_qs,
                    size_t n_dropped) const;
    stream_timestamp_t calc_queue_duration_() const;

    IReader& reader_;

    SortedQueue delay_queue_;
    stream_timestamp_t delay_;

    bool loaded_;
    bool unloaded_;

    const DelayedReaderConfig config_;

    // Latched to Depth on any fallback; further reads use the depth path.
    StartMode start_mode_;
    const char* start_fallback_reason_;

    CaptureTimestampMapping mapping_;
    // Whether a mapping was discarded as stale or implausible.
    bool mapping_rejected_;

    core::nanoseconds_t local_now_;
    core::nanoseconds_t wait_start_;

    const audio::SampleSpec sample_spec_;

    status::StatusCode init_status_;
};

} // namespace packet
} // namespace roc

#endif // ROC_PACKET_DELAYED_READER_H_
