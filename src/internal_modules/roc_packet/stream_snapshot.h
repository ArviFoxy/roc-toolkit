/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_packet/stream_snapshot.h
//! @brief Stream telemetry snapshot.

#ifndef ROC_PACKET_STREAM_SNAPSHOT_H_
#define ROC_PACKET_STREAM_SNAPSHOT_H_

#include "roc_core/stddefs.h"
#include "roc_core/time.h"
#include "roc_packet/units.h"

namespace roc {
namespace packet {

//! Maximum stream snapshots carried per report.
static const size_t MaxStreamSnapshots = 4;

//! One stream telemetry snapshot.
//!
//! The receiver captures a snapshot when its playback position crosses
//! a grid point on the sender clock timeline. The grid index identifies
//! the grid point. The same index is the idempotency key: a re-sent
//! snapshot with a known index is a no-op.
//!
//! This is the ONE in-memory form of the snapshot. The receiver
//! sampler (roc_audio), the RTCP report (roc_rtcp) and the sender
//! estimator (roc_pipeline) all use it, so a new field needs exactly
//! two edits: this struct and the wire block.
struct StreamSnapshot {
    //! Grid point index: floor(sender CTS / grid period).
    uint32_t grid_index;

    //! RTP stream timestamp of the grid point.
    stream_timestamp_t position;

    //! Queue latency at the crossing; negative if unavailable.
    core::nanoseconds_t niq_instant;

    //! Queue latency averaged over the grid interval; negative if unavailable.
    core::nanoseconds_t niq_mean;

    //! End-to-end latency at the crossing; negative if unavailable.
    core::nanoseconds_t e2e_latency;

    //! Whether warp_ppb carries a value.
    bool has_warp;

    //! Warp: (frequency coefficient - 1) in parts per billion.
    int32_t warp_ppb;

    //! Target latency at the crossing; negative if unavailable.
    core::nanoseconds_t target_latency;

    StreamSnapshot()
        : grid_index(0)
        , position(0)
        , niq_instant(-1)
        , niq_mean(-1)
        , e2e_latency(-1)
        , has_warp(false)
        , warp_ppb(0)
        , target_latency(-1) {
    }
};

} // namespace packet
} // namespace roc

#endif // ROC_PACKET_STREAM_SNAPSHOT_H_
