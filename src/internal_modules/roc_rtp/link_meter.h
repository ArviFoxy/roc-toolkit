/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_rtp/link_meter.h
//! @brief RTP link meter.

#ifndef ROC_RTP_LINK_METER_H_
#define ROC_RTP_LINK_METER_H_

#include "roc_audio/arrival_delay_meter.h"
#include "roc_audio/jitter_meter.h"
#include "roc_audio/sample_spec.h"
#include "roc_core/iarena.h"
#include "roc_core/noncopyable.h"
#include "roc_core/optional.h"
#include "roc_core/time.h"
#include "roc_dbgio/csv_dumper.h"
#include "roc_packet/ilink_meter.h"
#include "roc_packet/iwriter.h"
#include "roc_rtcp/reports.h"
#include "roc_rtp/encoding.h"
#include "roc_rtp/encoding_map.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/counter.h>
#endif

namespace roc {
namespace rtp {

//! RTP link meter.
//!
//! Computes various link metrics based on sequence of RTP packets.
//!
//! Inserted into pipeline as a writer, right after receiving packet, before storing
//! packet in incoming queue, which allows to update metrics as soon as new packets
//! arrive, without waiting until it's requested by depacketizer.
class LinkMeter : public packet::ILinkMeter,
                  public packet::IWriter,
                  public core::NonCopyable<> {
public:
    //! Initialize.
    //! @p delay_config enables the arrival delay meter; NULL disables it.
    //! The call site decides structurally: only the source (audio) meter
    //! of a session carries a delay meter. The repair meter passes NULL,
    //! both because repair packets have no own schedule on the media
    //! timeline and because a second instance would alias the unlabeled
    //! Prometheus series of the first.
    LinkMeter(packet::IWriter& writer,
              const audio::JitterMeterConfig& jitter_config,
              const audio::ArrivalDelayMeterConfig* delay_config,
              const EncodingMap& encoding_map,
              core::IArena& arena,
              dbgio::CsvDumper* dumper);

    //! Check if the object was successfully constructed.
    status::StatusCode init_status() const;

    //! Get arrival delay meter; NULL when disabled.
    audio::ArrivalDelayMeter* delay_meter();

    //! Check if metrics are already gathered and can be reported.
    virtual bool has_metrics() const;

    //! Get metrics.
    virtual const packet::LinkMetrics& metrics() const;

    //! Check if packet encoding already detected.
    bool has_encoding() const;

    //! Get detected encoding.
    //! @remarks
    //!  Panics if no encoding detected.
    const Encoding& encoding() const;

    //! Process RTCP report from sender.
    //! @remarks
    //!  Obtains additional information that can't be measured directly.
    void process_report(const rtcp::SendReport& report);

    //! Write packet and update metrics.
    //! @remarks
    //!  Invoked early in pipeline right after the packet is received.
    virtual ROC_NODISCARD status::StatusCode write(const packet::PacketPtr& packet);

private:
    void update_metrics_(const packet::Packet& packet);

    void update_seqnums_(const packet::Packet& packet);
    void update_jitter_(const packet::Packet& packet, core::nanoseconds_t d_s_ns);
    void update_delay_(const packet::Packet& packet, core::nanoseconds_t d_s_ns);

    void dump_(const packet::Packet& packet);

    const EncodingMap& encoding_map_;
    const Encoding* encoding_;

    packet::IWriter& writer_;

    bool first_packet_;

    bool has_metrics_;
    packet::LinkMetrics metrics_;

    uint16_t first_seqnum_;
    uint32_t last_seqnum_hi_;
    uint16_t last_seqnum_lo_;

    int64_t processed_packets_;
    core::nanoseconds_t prev_queue_timestamp_;
    packet::stream_timestamp_t prev_stream_timestamp_;

    // Arrival delay level reference: the delay level of a packet is
    // (queue_timestamp - first_queue_timestamp_) minus the packet's
    // stream position in nanoseconds relative to the first packet.
    // The stream position is kept as a forward-advancing accumulator
    // (stream_offset_ns_ is the position of the current anchor,
    // prev_stream_timestamp_): advancing with the anchor avoids the
    // wrap ambiguity of a fixed stream anchor, and the sub-ppm rounding
    // bias of per-advance conversion is absorbed by the meter baseline.
    core::nanoseconds_t first_queue_timestamp_;
    int64_t stream_offset_ns_;

    audio::JitterMeter jitter_meter_;
    core::Optional<audio::ArrivalDelayMeter> delay_meter_;

    status::StatusCode init_status_;

    dbgio::CsvDumper* dumper_;

#ifdef ROC_TARGET_PROMETHEUS
    uint64_t prom_prev_expected_;
    int64_t prom_prev_lost_;
    int64_t prom_prev_processed_;

    prometheus::Counter* expected_packets_counter_;
    prometheus::Counter* lost_packets_counter_;
    prometheus::Counter* received_packets_counter_;
#endif
};

} // namespace rtp
} // namespace roc

#endif // ROC_RTP_LINK_METER_H_
