/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_packet/capture_timestamp_mapping.h"
#include "roc_core/panic.h"

namespace roc {
namespace packet {

CaptureTimestampMapping::CaptureTimestampMapping(const audio::SampleSpec& sample_spec)
    : sample_spec_(sample_spec)
    , has_mapping_(false)
    , base_capture_ts_(0)
    , base_stream_ts_(0) {
}

bool CaptureTimestampMapping::has_mapping() const {
    return has_mapping_;
}

bool CaptureTimestampMapping::update(core::nanoseconds_t capture_ts,
                                     stream_timestamp_t stream_ts) {
    if (capture_ts <= 0) {
        return false;
    }

    has_mapping_ = true;
    base_capture_ts_ = capture_ts;
    base_stream_ts_ = stream_ts;

    return true;
}

void CaptureTimestampMapping::reset() {
    has_mapping_ = false;
    base_capture_ts_ = 0;
    base_stream_ts_ = 0;
}

core::nanoseconds_t CaptureTimestampMapping::base_capture_ts() const {
    return base_capture_ts_;
}

stream_timestamp_t CaptureTimestampMapping::base_stream_ts() const {
    return base_stream_ts_;
}

core::nanoseconds_t
CaptureTimestampMapping::capture_ts(stream_timestamp_t position) const {
    roc_panic_if_msg(!has_mapping_,
                     "capture timestamp mapping: conversion without reference pair");

    return base_capture_ts_
        + sample_spec_.stream_timestamp_delta_2_ns(
              stream_timestamp_diff(position, base_stream_ts_));
}

stream_timestamp_t
CaptureTimestampMapping::position(core::nanoseconds_t capture_ts) const {
    roc_panic_if_msg(!has_mapping_,
                     "capture timestamp mapping: conversion without reference pair");

    return base_stream_ts_
        + (stream_timestamp_t)sample_spec_.ns_2_stream_timestamp_delta(
              capture_ts - base_capture_ts_);
}

} // namespace packet
} // namespace roc
