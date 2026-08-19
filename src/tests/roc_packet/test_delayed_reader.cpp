/*
 * Copyright (c) 2015 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_core/heap_arena.h"
#include "roc_core/macro_helpers.h"
#include "roc_packet/delayed_reader.h"
#include "roc_packet/fifo_queue.h"
#include "roc_packet/packet_factory.h"
#include "roc_status/status_code.h"

namespace roc {
namespace packet {

namespace {

enum { SampleRate = 1000, NumSamples = 100, NumPackets = 30, MaxBufSize = 100 };

const core::nanoseconds_t NsPerSample = core::Second / SampleRate;

// Capture timestamp of stream position zero in the aligned-start tests.
const core::nanoseconds_t CtsBase = core::Second * 1000;

const audio::SampleSpec sample_spec(SampleRate,
                                    audio::PcmSubformat_Raw,
                                    audio::ChanLayout_Surround,
                                    audio::ChanOrder_Smpte,
                                    audio::ChanMask_Surround_Stereo);

core::HeapArena arena;
PacketFactory packet_factory(arena, MaxBufSize);

DelayedReaderConfig make_config(core::nanoseconds_t target_delay) {
    DelayedReaderConfig config;
    config.target_delay = target_delay;
    return config;
}

DelayedReaderConfig make_aligned_config() {
    DelayedReaderConfig config = make_config(NumSamples * NumPackets * NsPerSample);
    config.start_alignment = true;
    config.start_alignment_timeout = core::Second;
    config.cut_tolerance = core::Second * 2;
    return config;
}

PacketPtr new_packet(seqnum_t sn) {
    PacketPtr packet = packet_factory.new_packet();
    CHECK(packet);

    packet->add_flags(Packet::FlagRTP);
    packet->rtp()->seqnum = sn;
    packet->rtp()->stream_timestamp = stream_timestamp_t(sn * NumSamples);
    packet->rtp()->duration = NumSamples;

    return packet;
}

void write_packet(IWriter& writer, const PacketPtr& pp) {
    CHECK(pp);
    LONGS_EQUAL(status::StatusOK, writer.write(pp));
}

PacketPtr
expect_read(status::StatusCode expect_code, IReader& reader, PacketReadMode mode) {
    PacketPtr pp;
    LONGS_EQUAL(expect_code, reader.read(pp, mode));
    if (expect_code == status::StatusOK) {
        CHECK(pp);
    } else {
        CHECK(!pp);
    }
    return pp;
}

class MockReader : public IReader {
public:
    explicit MockReader(status::StatusCode code)
        : code_(code) {
    }

    virtual ROC_NODISCARD status::StatusCode read(PacketPtr& pp, PacketReadMode mode) {
        return code_;
    }

private:
    status::StatusCode code_;
};

} // namespace

TEST_GROUP(delayed_reader) {};

TEST(delayed_reader, no_delay) {
    FifoQueue queue;
    DelayedReader dr(queue, make_config(0), sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr wp = new_packet(n);
        write_packet(queue, wp);

        PacketPtr rp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(wp == rp);
    }
}

TEST(delayed_reader, delay) {
    FifoQueue queue;
    DelayedReader dr(queue, make_config(NumSamples * NumPackets * NsPerSample),
                     sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets];

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
        CHECK(!pp);

        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr wp = new_packet(NumPackets + n);
        write_packet(queue, wp);

        PacketPtr rp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(wp == rp);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, instant) {
    FifoQueue queue;
    DelayedReader dr(queue, make_config(NumSamples * NumPackets * NsPerSample),
                     sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets];

    for (seqnum_t n = 0; n < NumPackets; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, trim) {
    FifoQueue queue;
    DelayedReader dr(queue, make_config(NumSamples * NumPackets * NsPerSample),
                     sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    for (seqnum_t n = NumPackets; n < NumPackets * 2; n++) {
        PacketPtr pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, late_duplicates) {
    FifoQueue queue;
    DelayedReader dr(queue, make_config(NumSamples * NumPackets * NsPerSample),
                     sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets];

    for (seqnum_t n = 0; n < NumPackets; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr wp = new_packet(n);
        write_packet(queue, wp);

        PacketPtr rp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(wp == rp);
    }

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, fetch_peek) {
    FifoQueue queue;
    DelayedReader dr(queue, make_config(NumSamples * NumPackets * NsPerSample),
                     sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp;

        pp = expect_read(status::StatusDrain, dr, ModePeek);
        CHECK(!pp);

        pp = expect_read(status::StatusDrain, dr, ModeFetch);
        CHECK(!pp);

        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp;

        pp = expect_read(status::StatusOK, dr, ModePeek);
        CHECK(pp == packets[n]);

        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);

        packets[NumPackets + n] = new_packet(NumPackets + n);
        write_packet(queue, packets[NumPackets + n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        PacketPtr pp;

        pp = expect_read(status::StatusOK, dr, ModePeek);
        CHECK(pp == packets[NumPackets + n]);

        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[NumPackets + n]);
    }
}

TEST(delayed_reader, aligned_trim) {
    // The start lands on the packet containing the aligned position,
    // which differs from the depth-based cut.
    enum { AlignedOffset = 3350, FirstPacket = AlignedOffset / NumSamples };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    // Stream position 0 was captured at CtsBase; the local time places
    // the aligned position at AlignedOffset samples, inside packet 33,
    // while the depth-based cut would pick packet 30.
    dr.update_mapping(CtsBase, 0);
    dr.update_local_time(CtsBase + config.target_delay + AlignedOffset * NsPerSample);

    for (seqnum_t n = FirstPacket; n < NumPackets * 2; n++) {
        PacketPtr pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, aligned_waits_for_mapping) {
    // The queue is deep enough, but reads drain until the mapping
    // arrives; then the start is aligned.
    enum { AlignedOffset = 3350, FirstPacket = AlignedOffset / NumSamples };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    dr.update_local_time(CtsBase);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    // The mapping places the aligned position at AlignedOffset samples.
    dr.update_mapping(CtsBase - config.target_delay - AlignedOffset * NsPerSample, 0);

    for (seqnum_t n = FirstPacket; n < NumPackets * 2; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, mapping_timeout_fallback) {
    // No mapping ever arrives; when the pushed time passes the timeout,
    // the start is the exact depth-based trim of the `trim` test.
    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    dr.update_local_time(CtsBase);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    dr.update_local_time(CtsBase + config.start_alignment_timeout);

    for (seqnum_t n = NumPackets; n < NumPackets * 2; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, implausible_mapping_fallback) {
    // The aligned position is farther from the depth-based cut than the
    // cut tolerance: the mapping is discarded and reads drain; when the
    // pushed time passes the timeout, the depth-based start applies.
    enum { ImplausibleOffset = NumSamples * NumPackets * 3 + 1000 };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    const core::nanoseconds_t t0 =
        CtsBase + config.target_delay + ImplausibleOffset * NsPerSample;

    dr.update_mapping(CtsBase, 0);
    dr.update_local_time(t0);

    // The mapping is discarded; no further mapping arrives, so reads
    // drain until the timeout.
    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    dr.update_local_time(t0 + config.start_alignment_timeout);

    for (seqnum_t n = NumPackets; n < NumPackets * 2; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, good_mapping_after_implausible_mapping) {
    // An implausible mapping is discarded, then a good mapping arrives
    // within the timeout: the start is aligned as if the implausible
    // mapping never existed.
    enum {
        ImplausibleOffset = NumSamples * NumPackets * 3 + 1000,
        AlignedOffset = 3350,
        FirstPacket = AlignedOffset / NumSamples
    };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    const core::nanoseconds_t t0 =
        CtsBase + config.target_delay + ImplausibleOffset * NsPerSample;

    dr.update_mapping(CtsBase, 0);
    dr.update_local_time(t0);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    // The good mapping places the aligned position at AlignedOffset.
    dr.update_mapping(t0 - config.target_delay - AlignedOffset * NsPerSample, 0);

    for (seqnum_t n = FirstPacket; n < NumPackets * 2; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, stale_mapping_fallback) {
    // The mapping capture timestamp is implausibly far from the local
    // clock: the mapping is discarded and reads drain; when the pushed
    // time passes the timeout, the depth-based start applies.
    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    dr.update_mapping(CtsBase - core::Second * 70, 0);
    dr.update_local_time(CtsBase);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    dr.update_local_time(CtsBase + config.start_alignment_timeout);

    for (seqnum_t n = NumPackets; n < NumPackets * 2; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, good_mapping_after_stale_mapping) {
    // A stale mapping is discarded, then a good mapping arrives within
    // the timeout: the start is aligned.
    enum { AlignedOffset = 3350, FirstPacket = AlignedOffset / NumSamples };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    const core::nanoseconds_t t0 =
        CtsBase + config.target_delay + AlignedOffset * NsPerSample;

    dr.update_mapping(t0 - core::Second * 70, 0);
    dr.update_local_time(t0);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    // The good mapping places the aligned position at AlignedOffset.
    dr.update_mapping(CtsBase, 0);

    for (seqnum_t n = FirstPacket; n < NumPackets * 2; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, coverage_timeout_fallback) {
    // The mapping is plausible but the aligned position stays beyond the
    // stalled tail: on timeout the depth-based start is latched, and once
    // the queue accumulates the target delay, playback starts untrimmed.
    enum { QueuedPackets = 5, UncoveredOffset = QueuedPackets * NumSamples + 200 };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets];

    for (seqnum_t n = 0; n < QueuedPackets; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    const core::nanoseconds_t t0 =
        CtsBase + config.target_delay + UncoveredOffset * NsPerSample;

    dr.update_mapping(CtsBase, 0);
    dr.update_local_time(t0);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    // The timeout latches the depth-based start, which then waits for the
    // queue to accumulate the target delay.
    dr.update_local_time(t0 + config.start_alignment_timeout);

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    for (seqnum_t n = QueuedPackets; n < NumPackets; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    for (seqnum_t n = 0; n < NumPackets; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, head_start_when_p_star_precedes_queue) {
    // The aligned position stays before the queue head (the stream is
    // younger than the target delay); on timeout playback starts at the
    // head with nothing trimmed.
    enum { FirstSeqnum = 10, HeadOffset = FirstSeqnum * NumSamples };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets];

    for (seqnum_t n = 0; n < NumPackets; n++) {
        packets[n] = new_packet(FirstSeqnum + n);
        write_packet(queue, packets[n]);
    }

    // At the second pushed time, the aligned position is 500 samples:
    // still before the queue head at HeadOffset.
    const core::nanoseconds_t t0 = CtsBase;
    const core::nanoseconds_t t1 = t0 + config.start_alignment_timeout;

    dr.update_mapping(t1 - config.target_delay - 500 * NsPerSample, 0);
    dr.update_local_time(t0);

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);

    dr.update_local_time(t1);

    for (seqnum_t n = 0; n < NumPackets; n++) {
        pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, mapping_update_during_loading) {
    // Two mappings arrive during loading: the latest decides the aligned
    // position.
    enum {
        FirstOffset = 3350,
        MappingShift = 550,
        SecondOffset = FirstOffset + MappingShift,
        FirstPacket = SecondOffset / NumSamples
    };

    FifoQueue queue;
    DelayedReaderConfig config = make_aligned_config();
    DelayedReader dr(queue, config, sample_spec);
    LONGS_EQUAL(status::StatusOK, dr.init_status());

    PacketPtr packets[NumPackets * 2];

    for (seqnum_t n = 0; n < NumPackets * 2; n++) {
        packets[n] = new_packet(n);
        write_packet(queue, packets[n]);
    }

    // The first mapping would select FirstOffset; the second shifts the
    // capture timeline back, moving the aligned position to SecondOffset.
    dr.update_mapping(CtsBase, 0);
    dr.update_mapping(CtsBase - MappingShift * NsPerSample, 0);
    dr.update_local_time(CtsBase + config.target_delay + FirstOffset * NsPerSample);

    for (seqnum_t n = FirstPacket; n < NumPackets * 2; n++) {
        PacketPtr pp = expect_read(status::StatusOK, dr, ModeFetch);
        CHECK(pp == packets[n]);
    }

    PacketPtr pp = expect_read(status::StatusDrain, dr, ModeFetch);
    CHECK(!pp);
}

TEST(delayed_reader, forward_error) {
    const status::StatusCode status_list[] = {
        status::StatusDrain,
        status::StatusAbort,
    };

    const stream_timestamp_t delay_list[] = {
        0,
        NumSamples * NumPackets * NsPerSample,
    };

    for (size_t st_n = 0; st_n < ROC_ARRAY_SIZE(status_list); st_n++) {
        for (size_t dl_n = 0; dl_n < ROC_ARRAY_SIZE(delay_list); dl_n++) {
            MockReader reader(status_list[st_n]);
            DelayedReader dr(reader, make_config(delay_list[dl_n]), sample_spec);
            LONGS_EQUAL(status::StatusOK, dr.init_status());

            expect_read(status_list[st_n], dr, ModePeek);
            expect_read(status_list[st_n], dr, ModeFetch);
        }
    }
}

} // namespace packet
} // namespace roc
