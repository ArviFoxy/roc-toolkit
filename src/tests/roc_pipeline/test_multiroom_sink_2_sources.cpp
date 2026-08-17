/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "test_harness.h"
#include "test_helpers/frame_reader.h"
#include "test_helpers/frame_writer.h"

#include "roc_core/heap_arena.h"
#include "roc_core/optional.h"
#include "roc_core/slab_pool.h"
#include "roc_core/time.h"
#include "roc_packet/fifo_queue.h"
#include "roc_pipeline/receiver_source.h"
#include "roc_pipeline/sender_sink.h"
#include "roc_rtp/encoding_map.h"

// Offline integration test for the multiroom session sender: one SenderSink
// with a multitrack input and one slot per track, each slot connected to its
// own ReceiverSource through in-memory queues (no sockets, no threads, no
// clocks). Verifies that every receiver decodes exactly its slot's track and
// that all legs carry identical capture timestamps at the same stream
// position (the session stamps one CTS per frame for all legs).

namespace roc {
namespace pipeline {

namespace {

const audio::PcmSubformat Format_Raw = audio::PcmSubformat_Raw;

enum {
    MaxBufSize = 1000,

    SampleRate = 44100,

    SamplesPerFrame = 20,
    SamplesPerPacket = 100,
    FramesPerPacket = SamplesPerPacket / SamplesPerFrame,

    NumLegs = 3,

    Latency = SamplesPerPacket * 8,
    Timeout = Latency * 20,

    ManyFrames = FramesPerPacket * 60,
    // Two phases of ManyFrames (the RTCP test runs the loop twice).
    MaxPackets = 2 * ManyFrames / FramesPerPacket + 8,
};

core::HeapArena arena;

core::SlabPool<packet::Packet> packet_pool("packet_pool", arena);
core::SlabPool<core::Buffer>
    packet_buffer_pool("packet_buffer_pool", arena, sizeof(core::Buffer) + MaxBufSize);

core::SlabPool<audio::Frame> frame_pool("frame_pool", arena);
core::SlabPool<core::Buffer>
    frame_buffer_pool("frame_buffer_pool",
                      arena,
                      sizeof(core::Buffer) + MaxBufSize * sizeof(audio::sample_t));

packet::PacketFactory packet_factory(packet_pool, packet_buffer_pool);
audio::FrameFactory frame_factory(frame_pool, frame_buffer_pool);

audio::ProcessorMap processor_map(arena);
rtp::EncodingMap encoding_map(arena);

// Capture timestamps observed per packet position, shared by all legs:
// the first leg to reach a position records, the others must match.
struct CtsLog {
    core::nanoseconds_t cts[MaxPackets];
    bool valid[MaxPackets];

    CtsLog() {
        for (size_t n = 0; n < MaxPackets; n++) {
            cts[n] = 0;
            valid[n] = false;
        }
    }
};

// Moves packets of one leg from the sender's outbound queue into the
// receiver's inbound writer, stripping meta-data as if delivered over
// network, and checking the leg's capture timestamps against the log.
class LegDeliverer : core::NonCopyable<> {
public:
    LegDeliverer()
        : dst_writer_(NULL)
        , cts_log_(NULL)
        , pos_(0) {
    }

    void init(const address::SocketAddr& src_addr,
              packet::IWriter& dst_writer,
              CtsLog& cts_log) {
        src_addr_ = src_addr;
        dst_writer_ = &dst_writer;
        cts_log_ = &cts_log;
    }

    void deliver_from(packet::IReader& reader) {
        for (;;) {
            packet::PacketPtr pp;
            const status::StatusCode code = reader.read(pp, packet::ModeFetch);
            if (code != status::StatusOK) {
                LONGS_EQUAL(status::StatusDrain, code);
                break;
            }

            CHECK(pp->flags() & packet::Packet::FlagAudio);
            CHECK(pp->rtp());

            CHECK(pos_ < MaxPackets);
            if (!cts_log_->valid[pos_]) {
                cts_log_->cts[pos_] = pp->rtp()->capture_timestamp;
                cts_log_->valid[pos_] = true;
            } else {
                CHECK(cts_log_->cts[pos_] == pp->rtp()->capture_timestamp);
            }
            pos_++;

            LONGS_EQUAL(status::StatusOK, dst_writer_->write(strip_packet_(pp)));
        }
    }

    size_t n_delivered() const {
        return pos_;
    }

private:
    // New packet sharing the buffer but no meta-information, so the
    // receiver re-parses it as if it arrived from the network.
    packet::PacketPtr strip_packet_(const packet::PacketPtr& pa) {
        packet::PacketPtr pb = packet_factory.new_packet();
        CHECK(pb);

        CHECK(pa->flags() & packet::Packet::FlagUDP);
        pb->add_flags(packet::Packet::FlagUDP);
        *pb->udp() = *pa->udp();
        pb->udp()->src_addr = src_addr_;

        pb->set_buffer(pa->buffer());

        return pb;
    }

    address::SocketAddr src_addr_;
    packet::IWriter* dst_writer_;
    CtsLog* cts_log_;
    size_t pos_;
};

SenderSinkConfig make_sender_config() {
    SenderSinkConfig config;

    config.input_sample_spec.set_format(audio::Format_Pcm);
    config.input_sample_spec.set_pcm_subformat(Format_Raw);
    config.input_sample_spec.set_sample_rate(SampleRate);
    config.input_sample_spec.channel_set().set_layout(audio::ChanLayout_Multitrack);
    config.input_sample_spec.channel_set().set_order(audio::ChanOrder_None);
    config.input_sample_spec.channel_set().set_range(0, NumLegs - 1);

    config.payload_type = rtp::PayloadType_L16_Mono;
    config.packet_length = SamplesPerPacket * core::Second / SampleRate;

    config.enable_interleaving = false;
    config.enable_cpu_clock = false;
    config.enable_profiling = true;

    config.latency.tuner_backend = audio::LatencyTunerBackend_Niq;
    config.latency.tuner_profile = audio::LatencyTunerProfile_Intact;

    return config;
}

ReceiverSourceConfig make_receiver_config() {
    ReceiverSourceConfig config;

    config.common.output_sample_spec.set_format(audio::Format_Pcm);
    config.common.output_sample_spec.set_pcm_subformat(Format_Raw);
    config.common.output_sample_spec.set_sample_rate(SampleRate);
    config.common.output_sample_spec.channel_set().set_layout(
        audio::ChanLayout_Surround);
    config.common.output_sample_spec.channel_set().set_order(audio::ChanOrder_Smpte);
    config.common.output_sample_spec.channel_set().set_mask(
        audio::ChanMask_Surround_Mono);

    config.common.enable_cpu_clock = false;

    config.session_defaults.latency.tuner_backend = audio::LatencyTunerBackend_Niq;
    config.session_defaults.latency.tuner_profile = audio::LatencyTunerProfile_Intact;
    config.session_defaults.latency.target_latency = Latency * core::Second / SampleRate;
    config.session_defaults.watchdog.no_playback_timeout =
        Timeout * core::Second / SampleRate;

    return config;
}

// Forwards RTCP packets between peers, stripping meta-data like
// LegDeliverer but without audio-specific checks.
class ControlDeliverer : core::NonCopyable<> {
public:
    ControlDeliverer()
        : dst_writer_(NULL) {
    }

    void init(const address::SocketAddr& src_addr, packet::IWriter& dst_writer) {
        src_addr_ = src_addr;
        dst_writer_ = &dst_writer;
    }

    void deliver_from(packet::IReader& reader, bool drop = false) {
        for (;;) {
            packet::PacketPtr pp;
            const status::StatusCode code = reader.read(pp, packet::ModeFetch);
            if (code != status::StatusOK) {
                LONGS_EQUAL(status::StatusDrain, code);
                break;
            }

            if (drop) {
                continue;
            }

            packet::PacketPtr pb = packet_factory.new_packet();
            CHECK(pb);
            CHECK(pp->flags() & packet::Packet::FlagUDP);
            pb->add_flags(packet::Packet::FlagUDP);
            *pb->udp() = *pp->udp();
            pb->udp()->src_addr = src_addr_;
            pb->set_buffer(pp->buffer());

            LONGS_EQUAL(status::StatusOK, dst_writer_->write(pb));
        }
    }

private:
    address::SocketAddr src_addr_;
    packet::IWriter* dst_writer_;
};

SenderSlot* create_track_slot(SenderSink& sink, size_t track) {
    SenderSlotConfig slot_config;
    slot_config.enable_track_selection = true;
    slot_config.tracks.set_layout(audio::ChanLayout_Multitrack);
    slot_config.tracks.set_order(audio::ChanOrder_None);
    slot_config.tracks.set_range(track, track);

    SenderSlot* slot = sink.create_slot(slot_config);
    CHECK(slot);
    return slot;
}

} // namespace

TEST_GROUP(multiroom_sink_2_sources) {};

TEST(multiroom_sink_2_sources, track_per_leg) {
    const core::nanoseconds_t send_base_cts = 1000000000000000;

    SenderSink sender(make_sender_config(), processor_map, encoding_map, packet_pool,
                      packet_buffer_pool, frame_pool, frame_buffer_pool, arena);
    LONGS_EQUAL(status::StatusOK, sender.init_status());

    packet::FifoQueue leg_queues[NumLegs];
    core::Optional<ReceiverSource> receivers[NumLegs];
    core::Optional<test::FrameReader> frame_readers[NumLegs];
    LegDeliverer deliverers[NumLegs];
    CtsLog cts_log;

    for (size_t leg = 0; leg < NumLegs; leg++) {
        SenderSlot* sender_slot = create_track_slot(sender, leg);
        SenderEndpoint* sender_endpoint =
            sender_slot->add_endpoint(address::Iface_AudioSource, address::Proto_RTP,
                                      test::new_address(10 + (int)leg),
                                      leg_queues[leg]);
        CHECK(sender_endpoint);

        receivers[leg].reset(new (receivers[leg]) ReceiverSource(
            make_receiver_config(), processor_map, encoding_map, packet_pool,
            packet_buffer_pool, frame_pool, frame_buffer_pool, arena));
        LONGS_EQUAL(status::StatusOK, receivers[leg]->init_status());

        ReceiverSlotConfig receiver_slot_config;
        ReceiverSlot* receiver_slot =
            receivers[leg]->create_slot(receiver_slot_config);
        CHECK(receiver_slot);

        ReceiverEndpoint* receiver_endpoint =
            receiver_slot->add_endpoint(address::Iface_AudioSource, address::Proto_RTP,
                                        test::new_address(10 + (int)leg), NULL);
        CHECK(receiver_endpoint);

        deliverers[leg].init(test::new_address(44), receiver_endpoint->inbound_writer(),
                             cts_log);

        frame_readers[leg].reset(new (frame_readers[leg])
                                     test::FrameReader(*receivers[leg], frame_factory));
        frame_readers[leg]->expect_track(leg);
    }

    test::FrameWriter frame_writer(sender, frame_factory);

    const audio::SampleSpec input_spec = make_sender_config().input_sample_spec;
    const audio::SampleSpec output_spec = make_receiver_config().common.output_sample_spec;

    for (size_t nf = 0; nf < ManyFrames; nf++) {
        frame_writer.write_distinct_samples(SamplesPerFrame, input_spec, send_base_cts);

        LONGS_EQUAL(status::StatusOK,
                    sender.refresh(frame_writer.refresh_ts(send_base_cts), NULL));

        for (size_t leg = 0; leg < NumLegs; leg++) {
            deliverers[leg].deliver_from(leg_queues[leg]);
        }

        if (nf > Latency / SamplesPerFrame) {
            for (size_t leg = 0; leg < NumLegs; leg++) {
                LONGS_EQUAL(status::StatusOK,
                            receivers[leg]->refresh(
                                frame_readers[leg]->refresh_ts(-1), NULL));

                frame_readers[leg]->read_distinct_samples(SamplesPerFrame, output_spec);

                LONGS_EQUAL(1, receivers[leg]->num_sessions());
            }
        }
    }

    // Every leg delivered the same number of packets, and CTS was checked
    // for equality across legs at every packet position along the way.
    for (size_t leg = 1; leg < NumLegs; leg++) {
        LONGS_EQUAL(deliverers[0].n_delivered(), deliverers[leg].n_delivered());
    }
    CHECK(deliverers[0].n_delivered() > 0);
}

TEST(multiroom_sink_2_sources, rtcp_snapshots_reach_estimator) {
    // Same 3-leg session, now with per-leg RTCP control endpoints: the
    // receivers' stream snapshots flow back and the sender's skew
    // estimator finalizes rows at common grid points. Offline queues
    // give all legs identical timing, so clock-free offsets are ~0.
    const core::nanoseconds_t send_base_cts = 1000000000000000;

    SenderSinkConfig sender_config = make_sender_config();
    sender_config.rtcp.report_interval = SamplesPerPacket * core::Second / SampleRate;

    SenderSink sender(sender_config, processor_map, encoding_map, packet_pool,
                      packet_buffer_pool, frame_pool, frame_buffer_pool, arena);
    LONGS_EQUAL(status::StatusOK, sender.init_status());

    packet::FifoQueue leg_queues[NumLegs];
    packet::FifoQueue send_control_queues[NumLegs];
    packet::FifoQueue recv_control_queues[NumLegs];
    core::Optional<ReceiverSource> receivers[NumLegs];
    core::Optional<test::FrameReader> frame_readers[NumLegs];
    LegDeliverer deliverers[NumLegs];
    ControlDeliverer control_to_recv[NumLegs];
    ControlDeliverer control_to_send[NumLegs];
    CtsLog cts_log;

    for (size_t leg = 0; leg < NumLegs; leg++) {
        SenderSlot* sender_slot = create_track_slot(sender, leg);
        SenderEndpoint* sender_endpoint =
            sender_slot->add_endpoint(address::Iface_AudioSource, address::Proto_RTP,
                                      test::new_address(10 + (int)leg),
                                      leg_queues[leg]);
        CHECK(sender_endpoint);
        SenderEndpoint* sender_control_endpoint =
            sender_slot->add_endpoint(address::Iface_AudioControl, address::Proto_RTCP,
                                      test::new_address(60 + (int)leg),
                                      send_control_queues[leg]);
        CHECK(sender_control_endpoint);

        ReceiverSourceConfig receiver_config = make_receiver_config();
        receiver_config.common.rtcp.report_interval =
            SamplesPerPacket * core::Second / SampleRate;
        // Tiny grid so crossings fit into the short simulated stream.
        receiver_config.session_defaults.latency.snapshot_grid =
            10 * core::Millisecond;

        receivers[leg].reset(new (receivers[leg]) ReceiverSource(
            receiver_config, processor_map, encoding_map, packet_pool,
            packet_buffer_pool, frame_pool, frame_buffer_pool, arena));
        LONGS_EQUAL(status::StatusOK, receivers[leg]->init_status());

        ReceiverSlotConfig receiver_slot_config;
        ReceiverSlot* receiver_slot =
            receivers[leg]->create_slot(receiver_slot_config);
        CHECK(receiver_slot);

        ReceiverEndpoint* receiver_endpoint =
            receiver_slot->add_endpoint(address::Iface_AudioSource, address::Proto_RTP,
                                        test::new_address(10 + (int)leg), NULL);
        CHECK(receiver_endpoint);
        ReceiverEndpoint* receiver_control_endpoint = receiver_slot->add_endpoint(
            address::Iface_AudioControl, address::Proto_RTCP,
            test::new_address(60 + (int)leg), &recv_control_queues[leg]);
        CHECK(receiver_control_endpoint);

        deliverers[leg].init(test::new_address(44), receiver_endpoint->inbound_writer(),
                             cts_log);
        control_to_recv[leg].init(test::new_address(44),
                                  receiver_control_endpoint->inbound_writer());
        CHECK(sender_control_endpoint->inbound_writer());
        control_to_send[leg].init(test::new_address(80 + (int)leg),
                                  *sender_control_endpoint->inbound_writer());

        frame_readers[leg].reset(new (frame_readers[leg])
                                     test::FrameReader(*receivers[leg], frame_factory));
        frame_readers[leg]->expect_track(leg);
    }

    test::FrameWriter frame_writer(sender, frame_factory);

    const audio::SampleSpec input_spec = make_sender_config().input_sample_spec;
    const audio::SampleSpec output_spec =
        make_receiver_config().common.output_sample_spec;

    // Phase 1: everything flows.
    for (size_t nf = 0; nf < ManyFrames; nf++) {
        frame_writer.write_distinct_samples(SamplesPerFrame, input_spec, send_base_cts);

        LONGS_EQUAL(status::StatusOK,
                    sender.refresh(frame_writer.refresh_ts(send_base_cts), NULL));

        for (size_t leg = 0; leg < NumLegs; leg++) {
            deliverers[leg].deliver_from(leg_queues[leg]);
            control_to_recv[leg].deliver_from(send_control_queues[leg]);
        }

        if (nf > Latency / SamplesPerFrame) {
            for (size_t leg = 0; leg < NumLegs; leg++) {
                LONGS_EQUAL(status::StatusOK,
                            receivers[leg]->refresh(
                                frame_readers[leg]->refresh_ts(send_base_cts), NULL));

                frame_readers[leg]->read_distinct_samples(SamplesPerFrame, output_spec,
                                                          send_base_cts);

                control_to_send[leg].deliver_from(recv_control_queues[leg]);
            }
        }
    }

    SessionSkewEstimator& estimator = sender.skew_estimator();

    SessionSkewEstimator::FleetStats fleet;
    estimator.fleet_stats(fleet);
    CHECK(fleet.valid);
    CHECK(fleet.full_rows > 0);
    CHECK_EQUAL(0, fleet.rejected);

    // Offline legs run in lockstep: offsets and spread are tiny.
    CHECK(fleet.spread < 0.005);
    for (size_t leg = 0; leg < NumLegs; leg++) {
        SessionSkewEstimator::SlotStats slot_stats;
        CHECK(estimator.slot_stats(leg, slot_stats));
        CHECK(slot_stats.valid);
        CHECK(slot_stats.offset > -0.005 && slot_stats.offset < 0.005);
        CHECK(slot_stats.last_update > 0);
    }

    SessionSkewEstimator::PairStats pair;
    CHECK(estimator.pair_stats(0, 1, pair));
    CHECK(pair.valid);

    // Phase 2: leg 2's backchannel dies; rows finalize late without it
    // and its snapshots stop arriving while the others keep flowing.
    SessionSkewEstimator::SlotStats before_leg2;
    CHECK(estimator.slot_stats(2, before_leg2));
    const uint64_t partial_before = fleet.partial_rows;

    for (size_t nf = 0; nf < ManyFrames; nf++) {
        frame_writer.write_distinct_samples(SamplesPerFrame, input_spec, send_base_cts);

        LONGS_EQUAL(status::StatusOK,
                    sender.refresh(frame_writer.refresh_ts(send_base_cts), NULL));

        for (size_t leg = 0; leg < NumLegs; leg++) {
            deliverers[leg].deliver_from(leg_queues[leg]);
            control_to_recv[leg].deliver_from(send_control_queues[leg]);

            LONGS_EQUAL(status::StatusOK,
                        receivers[leg]->refresh(frame_readers[leg]->refresh_ts(send_base_cts),
                                                NULL));

            frame_readers[leg]->read_distinct_samples(SamplesPerFrame, output_spec,
                                                          send_base_cts);

            control_to_send[leg].deliver_from(recv_control_queues[leg],
                                              /* drop = */ leg == 2);
        }
    }

    estimator.fleet_stats(fleet);
    CHECK(fleet.partial_rows > partial_before);

    SessionSkewEstimator::SlotStats after_leg2, after_leg0;
    CHECK(estimator.slot_stats(2, after_leg2));
    CHECK(estimator.slot_stats(0, after_leg0));
    LONGLONGS_EQUAL(before_leg2.last_update, after_leg2.last_update);
    CHECK(after_leg0.last_update >= before_leg2.last_update);
}

} // namespace pipeline
} // namespace roc
