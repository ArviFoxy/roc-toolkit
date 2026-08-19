/*
 * Copyright (c) 2023 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_rtp/timestamp_injector.h"
#include "roc_core/log.h"
#include "roc_core/panic.h"
#include "roc_core/time.h"

namespace roc {
namespace rtp {

namespace {

const core::nanoseconds_t ReportInterval = core::Second * 30;

} // namespace

TimestampInjector::TimestampInjector(packet::IReader& reader,
                                     const audio::SampleSpec& sample_spec)
    : mapping_(sample_spec)
    , reader_(reader)
    , n_drops_(0)
    , rate_limiter_(ReportInterval, 1) {
}

status::StatusCode TimestampInjector::init_status() const {
    return status::StatusOK;
}

status::StatusCode TimestampInjector::read(packet::PacketPtr& pkt,
                                           packet::PacketReadMode mode) {
    const status::StatusCode code = reader_.read(pkt, mode);
    if (code != status::StatusOK) {
        return code;
    }

    if (!pkt->has_flags(packet::Packet::FlagRTP)) {
        roc_panic("timestamp injector: unexpected non-rtp packet");
    }

    if (mapping_.has_mapping()) {
        pkt->rtp()->capture_timestamp = mapping_.capture_ts(pkt->rtp()->stream_timestamp);
    }

    return status::StatusOK;
}

void TimestampInjector::update_mapping(core::nanoseconds_t capture_ts,
                                       packet::stream_timestamp_t rtp_ts) {
    if (rate_limiter_.allow()) {
        roc_log(LogDebug,
                "timestamp injector: received mapping:"
                " old=cts:%lld/sts:%llu new=cts:%lld/sts:%llu has_ts=%d n_drops=%lu",
                (long long)mapping_.base_capture_ts(),
                (unsigned long long)mapping_.base_stream_ts(), (long long)capture_ts,
                (unsigned long long)rtp_ts, (int)mapping_.has_mapping(),
                (unsigned long)n_drops_);
    }

    if (!mapping_.update(capture_ts, rtp_ts)) {
        roc_log(LogTrace, "timestamp injector: dropping mapping with negative cts");
        n_drops_++;
    }
}

} // namespace rtp
} // namespace roc
