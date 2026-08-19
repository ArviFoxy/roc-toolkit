/*
 * Copyright (c) 2015 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_packet/delayed_reader.h"
#include "roc_core/log.h"
#include "roc_core/panic.h"
#include "roc_core/time.h"
#include "roc_status/code_to_str.h"
#include "roc_status/status_code.h"

namespace roc {
namespace packet {

namespace {

// A mapping whose capture timestamp is this far from the local clock
// indicates broken clock synchronization. The bound also keeps the
// nanosecond distance to the mapping small enough that its conversion
// into a 32-bit sample delta cannot overflow.
const core::nanoseconds_t MaxMappingDistance = core::Second * 60;

} // namespace

DelayedReader::DelayedReader(IReader& reader,
                             const DelayedReaderConfig& config,
                             const audio::SampleSpec& sample_spec)
    : reader_(reader)
    , delay_queue_(0)
    , delay_(0)
    , loaded_(false)
    , unloaded_(false)
    , config_(config)
    , start_mode_(config.start_alignment ? StartAligned : StartDepth)
    , start_fallback_reason_("alignment_disabled")
    , mapping_(sample_spec)
    , mapping_rejected_(false)
    , local_now_(0)
    , wait_start_(0)
    , sample_spec_(sample_spec)
    , init_status_(status::NoStatus) {
    if (config.start_alignment && config.start_alignment_timeout <= 0) {
        roc_log(LogError,
                "delayed reader: start_alignment requires positive"
                " start_alignment_timeout");
        init_status_ = status::StatusBadConfig;
        return;
    }

    if (config.start_alignment && config.cut_tolerance <= 0) {
        roc_log(LogError, "delayed reader: start_alignment requires positive"
                          " cut_tolerance");
        init_status_ = status::StatusBadConfig;
        return;
    }

    if (config.target_delay > 0) {
        delay_ = sample_spec.ns_2_stream_timestamp(config.target_delay);
    }

    roc_log(LogDebug,
            "delayed reader: initializing: delay=%lu(%.3fms) start_alignment=%d",
            (unsigned long)delay_, sample_spec_.stream_timestamp_2_ms(delay_),
            (int)config.start_alignment);

    init_status_ = status::StatusOK;
}

status::StatusCode DelayedReader::init_status() const {
    return init_status_;
}

void DelayedReader::update_mapping(core::nanoseconds_t capture_ts,
                                   stream_timestamp_t rtp_ts) {
    roc_panic_if(init_status_ != status::StatusOK);

    mapping_.update(capture_ts, rtp_ts);
}

void DelayedReader::update_local_time(core::nanoseconds_t local_now) {
    roc_panic_if(init_status_ != status::StatusOK);

    if (local_now <= 0) {
        return;
    }

    local_now_ = local_now;

    if (wait_start_ == 0) {
        wait_start_ = local_now;
    }
}

status::StatusCode DelayedReader::read(PacketPtr& packet, PacketReadMode mode) {
    roc_panic_if(init_status_ != status::StatusOK);

    if (!loaded_) {
        const status::StatusCode code = load_queue_();
        if (code != status::StatusOK) {
            return code;
        }
        loaded_ = true;
    }

    if (!unloaded_) {
        if (delay_queue_.size() != 0) {
            return delay_queue_.read(packet, mode);
        }
        unloaded_ = true;
    }

    return reader_.read(packet, mode);
}

status::StatusCode DelayedReader::load_queue_() {
    // fetch all available packets into queue
    PacketPtr pp;
    for (;;) {
        status::StatusCode code = status::NoStatus;

        if ((code = reader_.read(pp, ModeFetch)) != status::StatusOK) {
            if (code == status::StatusDrain) {
                break;
            }
            return code;
        }

        if ((code = delay_queue_.write(pp)) != status::StatusOK) {
            return code;
        }
    }

    if (start_mode_ == StartAligned) {
        const status::StatusCode code = aligned_start_();
        if (start_mode_ == StartAligned) {
            return code;
        }
        // A fallback latched the depth-based start: continue below.
    }

    const stream_timestamp_t init_qs = calc_queue_duration_();
    if (init_qs < delay_) {
        // return drain until queue is large enough
        return status::StatusDrain;
    }

    // trim queue if it's too big
    size_t n_dropped = 0;
    if (delay_queue_.size() != 0) {
        const stream_timestamp_t tail_end =
            delay_queue_.tail()->stream_timestamp() + delay_queue_.tail()->duration();

        const status::StatusCode code = trim_to_(tail_end - delay_, n_dropped);
        if (code != status::StatusOK) {
            return code;
        }
    }

    log_start_("depth", start_fallback_reason_, 0, init_qs, n_dropped);

    return status::StatusOK;
}

// Start position from the wall clock: the queue head becomes the packet
// containing the position captured target_delay before the local time
// (whole-packet granularity, at most one packet early). Returns
// StatusDrain while waiting for a usable mapping or for queue coverage,
// and StatusOK once playback starts. A stale or implausible mapping is
// discarded, so a later mapping within the timeout can still align the
// start. Fallbacks latch start_mode_ to StartDepth and leave the queue
// for the depth-based path, which runs within the same read.
status::StatusCode DelayedReader::aligned_start_() {
    // Local time was never pushed: the timeout is not armed yet.
    if (local_now_ == 0) {
        return status::StatusDrain;
    }

    const bool timed_out = local_now_ - wait_start_ >= config_.start_alignment_timeout;

    // Broken clock synchronization detector: a valid mapping extrapolated
    // to the present moment stays within network + queueing distance of
    // the local clock.
    if (mapping_.has_mapping()
        && std::abs(local_now_ - mapping_.base_capture_ts()) > MaxMappingDistance) {
        roc_log(LogError,
                "delayed reader: mapping capture timestamp is implausibly far"
                " from local clock, discarding mapping:"
                " local_now=%lld map_cts=%lld",
                (long long)local_now_, (long long)mapping_.base_capture_ts());
        mapping_.reset();
        mapping_rejected_ = true;
    }

    if (!mapping_.has_mapping()) {
        if (timed_out) {
            roc_log(LogNote,
                    "delayed reader: no usable capture timestamp mapping within"
                    " timeout, falling back to depth-based start: waited=%.3fms",
                    (double)(local_now_ - wait_start_) / core::Millisecond);
            start_mode_ = StartDepth;
            start_fallback_reason_ =
                mapping_rejected_ ? "no_usable_mapping" : "no_mapping";
        }
        return status::StatusDrain;
    }

    if (delay_queue_.size() == 0) {
        return status::StatusDrain;
    }

    // Aligned start position: the stream position captured target_delay
    // before the local time. Inverse of the extrapolation the timestamp
    // injector applies downstream.
    const stream_timestamp_t p_star = mapping_.position(local_now_ - config_.target_delay);

    const stream_timestamp_t head = delay_queue_.head()->stream_timestamp();
    const stream_timestamp_t tail_end =
        delay_queue_.tail()->stream_timestamp() + delay_queue_.tail()->duration();

    // The cut the depth-based start would choose, as plausibility anchor.
    const stream_timestamp_t depth_cut =
        calc_queue_duration_() >= delay_ ? tail_end - delay_ : head;

    // Distrust and discard the mapping when the clock-derived cut is
    // farther from the depth-based cut than the tuner tolerance: the
    // latency tuner would abort the session from such a start anyway.
    // Also absorbs local clock steps during the wait.
    const stream_timestamp_diff_t cut_offset = stream_timestamp_diff(p_star, depth_cut);
    if (std::abs(sample_spec_.stream_timestamp_delta_2_ns(cut_offset))
        > config_.cut_tolerance) {
        roc_log(LogError,
                "delayed reader: aligned start position is implausibly far"
                " from depth-based cut, discarding mapping:"
                " cut_offset=%+.3fms cut_tolerance=%.3fms",
                sample_spec_.stream_timestamp_delta_2_ms(cut_offset),
                (double)config_.cut_tolerance / core::Millisecond);
        mapping_.reset();
        mapping_rejected_ = true;
        return status::StatusDrain;
    }

    if (stream_timestamp_ge(p_star, head) && stream_timestamp_lt(p_star, tail_end)) {
        // The queue covers the aligned position: drop head packets that
        // end at or before it.
        const stream_timestamp_t init_qs = calc_queue_duration_();

        size_t n_dropped = 0;
        const status::StatusCode code = trim_to_(p_star, n_dropped);
        if (code != status::StatusOK) {
            return code;
        }

        log_start_("aligned", "", cut_offset, init_qs, n_dropped);

        return status::StatusOK;
    }

    if (timed_out) {
        if (stream_timestamp_lt(p_star, head)) {
            // The stream is younger than the target delay: the head is the
            // closest available position, and the remaining error is small
            // enough for the latency tuner to absorb.
            log_start_("head", "stream_younger_than_target", cut_offset,
                       calc_queue_duration_(), 0);
            return status::StatusOK;
        }

        roc_log(LogNote,
                "delayed reader: queue did not cover aligned start position"
                " within timeout, falling back to depth-based start:"
                " waited=%.3fms cut_offset=%+.3fms",
                (double)(local_now_ - wait_start_) / core::Millisecond,
                sample_spec_.stream_timestamp_delta_2_ms(cut_offset));
        start_mode_ = StartDepth;
        start_fallback_reason_ = "coverage_timeout";
        return status::StatusDrain;
    }

    // Not covered yet: the aligned position advances at wall rate against
    // a fixed head, so a fresh queue becomes coverable within the target
    // delay.
    return status::StatusDrain;
}

// Drops head packets that end at or before the cut. Keeps at least one
// packet, so the queue never empties.
status::StatusCode DelayedReader::trim_to_(stream_timestamp_t cut, size_t& n_dropped) {
    n_dropped = 0;

    while (delay_queue_.size() > 1
           && stream_timestamp_le(delay_queue_.head()->stream_timestamp()
                                      + delay_queue_.head()->duration(),
                                  cut)) {
        PacketPtr pp;
        const status::StatusCode code = delay_queue_.read(pp, ModeFetch);
        if (code != status::StatusOK) {
            return code;
        }
        n_dropped++;
    }

    return status::StatusOK;
}

void DelayedReader::log_start_(const char* mode,
                               const char* reason,
                               stream_timestamp_diff_t cut_offset,
                               stream_timestamp_t init_qs,
                               size_t n_dropped) const {
    const stream_timestamp_t trim_qs = calc_queue_duration_();

    roc_log(LogNote,
            "delayed reader: starting:"
            " mode=%s reason=%s cut_offset=%+.3fms"
            " delay=%lu(%.3fms) init_qs=%lu(%.3fms) trim_qs=%lu(%.3fms)"
            " n_drop=%lu n_keep=%lu",
            mode, reason != NULL && reason[0] != '\0' ? reason : "none",
            sample_spec_.stream_timestamp_delta_2_ms(cut_offset), (unsigned long)delay_,
            sample_spec_.stream_timestamp_2_ms(delay_), (unsigned long)init_qs,
            sample_spec_.stream_timestamp_2_ms(init_qs), (unsigned long)trim_qs,
            sample_spec_.stream_timestamp_2_ms(trim_qs), (unsigned long)n_dropped,
            (unsigned long)delay_queue_.size());
}

stream_timestamp_t DelayedReader::calc_queue_duration_() const {
    if (delay_queue_.size() == 0) {
        return 0;
    }

    const stream_timestamp_diff_t qs = stream_timestamp_diff(
        delay_queue_.tail()->stream_timestamp() + delay_queue_.tail()->duration(),
        delay_queue_.head()->stream_timestamp());

    if (qs < 0) {
        roc_log(LogError, "delayed reader: unexpected negative queue size: %ld",
                (long)qs);
        return 0;
    }

    return (stream_timestamp_t)qs;
}

} // namespace packet
} // namespace roc
