/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_packet/capture_timestamp_mapping.h
//! @brief Capture timestamp mapping.

#ifndef ROC_PACKET_CAPTURE_TIMESTAMP_MAPPING_H_
#define ROC_PACKET_CAPTURE_TIMESTAMP_MAPPING_H_

#include "roc_audio/sample_spec.h"
#include "roc_core/time.h"
#include "roc_packet/units.h"

namespace roc {
namespace packet {

//! Mapping between capture timestamps and stream timestamps.
//!
//! Holds one reference pair (capture timestamp, stream timestamp), as
//! delivered by RTCP sender reports, and converts between the two
//! timelines by extrapolating from the pair at the nominal sample rate.
//! Conversions are wrap-safe with respect to the 32-bit stream timestamp.
class CaptureTimestampMapping {
public:
    //! Initialize.
    explicit CaptureTimestampMapping(const audio::SampleSpec& sample_spec);

    //! Check if a reference pair was set.
    bool has_mapping() const;

    //! Set the reference pair: @p capture_ts corresponds to @p stream_ts.
    //! The latest pair wins. Returns false and keeps the previous pair
    //! if @p capture_ts is not positive.
    bool update(core::nanoseconds_t capture_ts, stream_timestamp_t stream_ts);

    //! Forget the reference pair.
    void reset();

    //! Capture timestamp of the reference pair (zero if none was set).
    core::nanoseconds_t base_capture_ts() const;

    //! Stream timestamp of the reference pair (zero if none was set).
    stream_timestamp_t base_stream_ts() const;

    //! Convert stream position to capture timestamp.
    //! @pre a reference pair was set.
    core::nanoseconds_t capture_ts(stream_timestamp_t position) const;

    //! Convert capture timestamp to stream position.
    //! @pre a reference pair was set.
    stream_timestamp_t position(core::nanoseconds_t capture_ts) const;

private:
    const audio::SampleSpec sample_spec_;

    bool has_mapping_;
    core::nanoseconds_t base_capture_ts_;
    stream_timestamp_t base_stream_ts_;
};

} // namespace packet
} // namespace roc

#endif // ROC_PACKET_CAPTURE_TIMESTAMP_MAPPING_H_
