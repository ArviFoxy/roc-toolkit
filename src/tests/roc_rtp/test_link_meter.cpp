/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "test_helpers/status_writer.h"

#include "roc_audio/latency_monitor.h"
#include "roc_audio/sample_spec.h"
#include "roc_core/fast_random.h"
#include "roc_core/heap_arena.h"
#include "roc_core/macro_helpers.h"
#include "roc_packet/fifo_queue.h"
#include "roc_packet/packet_factory.h"
#include "roc_pipeline/config.h"
#include "roc_rtp/encoding_map.h"
#include "roc_rtp/headers.h"
#include "roc_rtp/link_meter.h"

namespace roc {
namespace rtp {

namespace {

enum {
    ChMask = 3,
    PacketSz = 100,
    SampleRate = 44100,
    Duration = 44,
    RunningWindowLen = 1000
};

core::HeapArena arena;
packet::PacketFactory packet_factory(arena, PacketSz);

EncodingMap encoding_map(arena);

audio::SampleSpec sample_spec(SampleRate,
                              audio::PcmSubformat_Raw,
                              audio::ChanLayout_Surround,
                              audio::ChanOrder_Smpte,
                              ChMask);

const core::nanoseconds_t qts_start = 1691499037871419405;
const core::nanoseconds_t qts_step = Duration * core::Second / SampleRate;

const packet::stream_timestamp_t sts_start = 6134803;
const packet::stream_timestamp_t sts_step = Duration;

packet::PacketPtr new_packet(packet::seqnum_t sn,
                             const core::nanoseconds_t queue_ts,
                             const packet::stream_timestamp_t stream_ts) {
    packet::PacketPtr packet = packet_factory.new_packet();
    CHECK(packet);

    packet->add_flags(packet::Packet::FlagRTP | packet::Packet::FlagUDP);
    packet->rtp()->payload_type = PayloadType_L16_Stereo;
    packet->rtp()->seqnum = sn;
    packet->rtp()->duration = Duration;
    packet->rtp()->stream_timestamp = stream_ts;
    packet->udp()->queue_timestamp = queue_ts;

    return packet;
}

audio::JitterMeterConfig make_config() {
    audio::JitterMeterConfig config;
    config.jitter_window = RunningWindowLen;
    config.peak_quantile_window = RunningWindowLen / 5;
    config.envelope_resistance_coeff = 0.1;
    return config;
}

} // namespace

TEST_GROUP(link_meter) {};

TEST(link_meter, has_metrics) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    CHECK(!meter.has_metrics());

    LONGS_EQUAL(status::StatusOK, meter.write(new_packet(100, qts_start, sts_start)));
    UNSIGNED_LONGS_EQUAL(1, queue.size());

    CHECK(meter.has_metrics());
}

TEST(link_meter, last_seqnum) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    UNSIGNED_LONGS_EQUAL(0, meter.metrics().ext_last_seqnum);

    LONGS_EQUAL(status::StatusOK, meter.write(new_packet(100, qts, sts)));
    UNSIGNED_LONGS_EQUAL(100, meter.metrics().ext_last_seqnum);
    qts += qts_step;
    sts += sts_step;

    // seqnum increased, metric updated
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(102, qts + qts_step, sts + sts_step)));
    UNSIGNED_LONGS_EQUAL(102, meter.metrics().ext_last_seqnum);

    // seqnum decreased, ignored
    LONGS_EQUAL(status::StatusOK, meter.write(new_packet(101, qts, sts)));
    UNSIGNED_LONGS_EQUAL(102, meter.metrics().ext_last_seqnum);
    qts += qts_step * 2;
    sts += sts_step * 2;

    // seqnum increased, metric updated
    UNSIGNED_LONGS_EQUAL(status::StatusOK, meter.write(new_packet(103, qts, sts)));
    UNSIGNED_LONGS_EQUAL(103, meter.metrics().ext_last_seqnum);

    UNSIGNED_LONGS_EQUAL(4, queue.size());
}

TEST(link_meter, last_seqnum_wrap) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    UNSIGNED_LONGS_EQUAL(0, meter.metrics().ext_last_seqnum);

    // no overflow
    LONGS_EQUAL(status::StatusOK, meter.write(new_packet(65533, qts, sts)));
    UNSIGNED_LONGS_EQUAL(65533, meter.metrics().ext_last_seqnum);
    UNSIGNED_LONGS_EQUAL(1, meter.metrics().expected_packets);

    // no overflow
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(65535, qts + qts_step * 2, sts + sts_step * 2)));
    UNSIGNED_LONGS_EQUAL(65535, meter.metrics().ext_last_seqnum);
    UNSIGNED_LONGS_EQUAL(3, meter.metrics().expected_packets);

    // overflow
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(1, qts + qts_step * 3, sts + sts_step * 3)));
    UNSIGNED_LONGS_EQUAL(65537, meter.metrics().ext_last_seqnum);
    UNSIGNED_LONGS_EQUAL(5, meter.metrics().expected_packets);

    // late packet, ignored
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(65534, qts + qts_step, sts + sts_step)));
    UNSIGNED_LONGS_EQUAL(65537, meter.metrics().ext_last_seqnum);
    UNSIGNED_LONGS_EQUAL(5, meter.metrics().expected_packets);

    // new packet
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(4, qts + qts_step * 6, sts + sts_step * 6)));
    UNSIGNED_LONGS_EQUAL(65540, meter.metrics().ext_last_seqnum);
    UNSIGNED_LONGS_EQUAL(8, meter.metrics().expected_packets);

    UNSIGNED_LONGS_EQUAL(5, queue.size());
}

TEST(link_meter, jitter_test) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    const size_t num_packets = Duration * 100;
    core::nanoseconds_t ts_store[num_packets];

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    for (size_t i = 0; i < num_packets; i++) {
        packet::seqnum_t seqnum = 65500 + i;
        ts_store[i] = qts;
        UNSIGNED_LONGS_EQUAL(status::StatusOK, meter.write(new_packet(seqnum, qts, sts)));
        const core::nanoseconds_t jitter_ns =
            (core::nanoseconds_t)(core::fast_random_gaussian() * core::Millisecond);
        qts += qts_step + jitter_ns;
        sts += sts_step;

        if (i > RunningWindowLen) {
            // Check meter metrics running max in min jitter in last Duration number
            // of packets in ts_store.
            core::nanoseconds_t peak_jitter = 0;
            for (size_t j = 0; j < RunningWindowLen; j++) {
                core::nanoseconds_t jitter =
                    std::abs(ts_store[i - j] - ts_store[i - j - 1] - qts_step);
                peak_jitter = std::max(peak_jitter, jitter);
            }
            DOUBLES_EQUAL(peak_jitter, meter.metrics().peak_jitter,
                          core::Millisecond * 3);

            // Reference average  and variance of jitter from ts_store values.
            core::nanoseconds_t sum = 0;
            for (size_t j = 0; j < RunningWindowLen; j++) {
                sum += std::abs(ts_store[i - j] - ts_store[i - j - 1] - qts_step);
            }
            const core::nanoseconds_t mean = sum / RunningWindowLen;

            sum = 0;
            for (size_t j = 0; j < RunningWindowLen; j++) {
                core::nanoseconds_t jitter =
                    std::abs(ts_store[i - j] - ts_store[i - j - 1] - qts_step);
                sum += (jitter - mean) * (jitter - mean);
            }

            // Check the jitter value
            DOUBLES_EQUAL(mean, meter.metrics().mean_jitter, core::Microsecond * 1);
        }
    }
}

TEST(link_meter, ascending_test) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    const size_t num_packets = Duration * 100;
    core::nanoseconds_t ts_store[num_packets];

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    for (size_t i = 0; i < num_packets; i++) {
        packet::seqnum_t seqnum = 65500 + i;
        ts_store[i] = qts;
        UNSIGNED_LONGS_EQUAL(status::StatusOK, meter.write(new_packet(seqnum, qts, sts)));

        // Removed the random component to create an increasing sequence
        qts += qts_step + (core::nanoseconds_t)i * core::Microsecond;
        sts += sts_step;

        if (i > RunningWindowLen) {
            // Check meter metrics running max in min jitter in last Duration number
            // of packets in ts_store.
            core::nanoseconds_t peak_jitter = 0;
            for (size_t j = 0; j < RunningWindowLen; j++) {
                core::nanoseconds_t jitter =
                    std::abs(ts_store[i - j] - ts_store[i - j - 1] - qts_step);
                peak_jitter = std::max(peak_jitter, jitter);
            }
            DOUBLES_EQUAL(peak_jitter, meter.metrics().peak_jitter,
                          core::Millisecond * 3);
        }
    }
}

TEST(link_meter, descending_test) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    const size_t num_packets = Duration * 100;
    core::nanoseconds_t ts_store[num_packets];

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    for (size_t i = 0; i < num_packets; i++) {
        packet::seqnum_t seqnum = 65500 + i;
        ts_store[i] = qts;
        UNSIGNED_LONGS_EQUAL(status::StatusOK, meter.write(new_packet(seqnum, qts, sts)));

        // Removed the random component to create an decreasing sequence
        qts += qts_step - (core::nanoseconds_t)i * core::Nanosecond * 10;
        sts += sts_step;

        if (i > RunningWindowLen) {
            // Check meter metrics running max in min jitter in last Duration number
            // of packets in ts_store.
            core::nanoseconds_t peak_jitter = 0;
            for (size_t j = 0; j < RunningWindowLen; j++) {
                core::nanoseconds_t jitter =
                    std::abs(ts_store[i - j] - ts_store[i - j - 1] - qts_step);
                peak_jitter = std::max(peak_jitter, jitter);
            }
            DOUBLES_EQUAL(peak_jitter, meter.metrics().peak_jitter,
                          core::Millisecond * 3);
        }
    }
}

TEST(link_meter, saw_test) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    const size_t num_packets = Duration * 100;
    core::nanoseconds_t ts_store[num_packets];
    core::nanoseconds_t step_ts_inc = core::Nanosecond * 10;
    core::nanoseconds_t step_ts_ = qts_step;

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    for (size_t i = 0; i < num_packets; i++) {
        packet::seqnum_t seqnum = 65500 + i;
        ts_store[i] = qts;
        UNSIGNED_LONGS_EQUAL(status::StatusOK, meter.write(new_packet(seqnum, qts, sts)));
        qts += step_ts_;
        sts += sts_step;
        step_ts_ += step_ts_inc;
        if (i > 0 && i % RunningWindowLen == 0) {
            step_ts_inc = -step_ts_inc;
        }

        if (i > RunningWindowLen) {
            // Check meter metrics running max in min jitter in last Duration number
            // of packets in ts_store.
            core::nanoseconds_t peak_jitter = 0;
            for (size_t j = 0; j < RunningWindowLen; j++) {
                core::nanoseconds_t jitter =
                    std::abs(ts_store[i - j] - ts_store[i - j - 1] - qts_step);
                peak_jitter = std::max(peak_jitter, jitter);
            }
            DOUBLES_EQUAL(peak_jitter, meter.metrics().peak_jitter,
                          core::Millisecond * 3);
        }
    }
}

TEST(link_meter, losses_test) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    const size_t num_packets = Duration * 2 * (1 << 16);
    int64_t total_losses = 0;

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;

    for (size_t i = 0; i < num_packets; i++) {
        packet::seqnum_t seqnum = 65500 + i;
        packet::PacketPtr p = new_packet(seqnum, qts, sts);
        qts += qts_step;
        sts += sts_step;

        if (i > 0 && core::fast_random_range(0, 100) < 30) {
            i += 99;
            total_losses += 100;
            continue;
        } else {
            UNSIGNED_LONGS_EQUAL(status::StatusOK, meter.write(p));
        }

        packet::PacketPtr pr;
        UNSIGNED_LONGS_EQUAL(status::StatusOK, queue.read(pr, packet::ModeFetch));
        UNSIGNED_LONGS_EQUAL(pr->rtp()->seqnum, p->rtp()->seqnum);

        if (i > 0) {
            const packet::LinkMetrics& metrics = meter.metrics();
            UNSIGNED_LONGS_EQUAL(total_losses, metrics.lost_packets);
            UNSIGNED_LONGS_EQUAL(i + 1, metrics.expected_packets);
        }
    }
}

TEST(link_meter, total_counter) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);

    core::nanoseconds_t ts = qts_start;
    packet::stream_timestamp_t sts = sts_start;
    uint16_t seqnum = 65500;
    uint32_t total_counter = 0;

    UNSIGNED_LONGS_EQUAL(0, meter.metrics().ext_last_seqnum);

    for (size_t i = 0; i < 66000; i++) {
        LONGS_EQUAL(status::StatusOK,
                    meter.write(new_packet(uint16_t((seqnum + total_counter) & 0xFFFF),
                                           ts + qts_step * total_counter,
                                           sts + sts_step * total_counter)));
        UNSIGNED_LONGS_EQUAL(uint32_t(seqnum) + total_counter,
                             meter.metrics().ext_last_seqnum);
        UNSIGNED_LONGS_EQUAL(total_counter + 1, meter.metrics().expected_packets);

        UNSIGNED_LONGS_EQUAL(i + 1, queue.size());

        total_counter += 1;
    }
}

TEST(link_meter, forward_error) {
    const status::StatusCode status_list[] = {
        status::StatusErrDevice,
        status::StatusErrFile,
    };

    for (size_t st_n = 0; st_n < ROC_ARRAY_SIZE(status_list); st_n++) {
        test::StatusWriter writer(status_list[st_n]);
        LinkMeter meter(writer, make_config(), NULL, encoding_map, arena, NULL);

        LONGS_EQUAL(status_list[st_n],
                    meter.write(new_packet(100, qts_start, sts_start)));
    }
}

TEST(link_meter, delay_meter_disabled) {
    packet::FifoQueue queue;
    LinkMeter meter(queue, make_config(), NULL, encoding_map, arena, NULL);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    POINTERS_EQUAL(NULL, meter.delay_meter());

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;
    for (packet::seqnum_t sn = 0; sn < 10; sn++) {
        LONGS_EQUAL(status::StatusOK, meter.write(new_packet(sn, qts, sts)));
        qts += qts_step;
        sts += sts_step;
    }
    POINTERS_EQUAL(NULL, meter.delay_meter());
}

TEST(link_meter, delay_level_flat) {
    packet::FifoQueue queue;
    audio::ArrivalDelayMeterConfig delay_config;
    LinkMeter meter(queue, make_config(), &delay_config, encoding_map, arena, NULL);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    CHECK(meter.delay_meter() != NULL);

    // Arrival spacing equals stream spacing: the level stays near zero
    // (only per-advance rounding of the stream-to-nanoseconds
    // conversion remains).
    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;
    for (packet::seqnum_t sn = 0; sn < 200; sn++) {
        LONGS_EQUAL(status::StatusOK, meter.write(new_packet(sn, qts, sts)));
        qts += qts_step;
        sts += sts_step;
    }

    const audio::ArrivalDelayMetrics& metrics = meter.delay_meter()->metrics();
    CHECK(metrics.curr_deviation < 10 * core::Microsecond);
    CHECK(metrics.baseline > -10 * core::Microsecond);
    CHECK(metrics.baseline < 10 * core::Microsecond);
}

TEST(link_meter, delay_level_late_packet) {
    packet::FifoQueue queue;
    audio::ArrivalDelayMeterConfig delay_config;
    LinkMeter meter(queue, make_config(), &delay_config, encoding_map, arena, NULL);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;
    packet::seqnum_t sn = 0;
    for (; sn < 100; sn++) {
        LONGS_EQUAL(status::StatusOK, meter.write(new_packet(sn, qts, sts)));
        qts += qts_step;
        sts += sts_step;
    }

    // A packet delayed by 5 ms on the wire: same schedule, late arrival.
    const core::nanoseconds_t lateness = 5 * core::Millisecond;
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(sn, qts + lateness, sts)));

    const audio::ArrivalDelayMetrics& metrics = meter.delay_meter()->metrics();
    CHECK(metrics.curr_deviation > lateness - 100 * core::Microsecond);
    CHECK(metrics.curr_deviation < lateness + 100 * core::Microsecond);
}

TEST(link_meter, delay_level_reordered_packet) {
    packet::FifoQueue queue;
    audio::ArrivalDelayMeterConfig delay_config;
    LinkMeter meter(queue, make_config(), &delay_config, encoding_map, arena, NULL);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = sts_start;
    packet::seqnum_t sn = 0;
    for (; sn < 100; sn++) {
        LONGS_EQUAL(status::StatusOK, meter.write(new_packet(sn, qts, sts)));
        qts += qts_step;
        sts += sts_step;
    }

    // A reordered packet: stream position 3 packets back, arriving now.
    // Its deviation is its own lateness (3 periods); the anchors must
    // not move, so the next in-order packet is back at level zero.
    LONGS_EQUAL(status::StatusOK,
                meter.write(new_packet(sn - 3, qts, sts - 3 * sts_step)));

    const audio::ArrivalDelayMetrics& metrics = meter.delay_meter()->metrics();
    CHECK(metrics.curr_deviation > 3 * qts_step - 100 * core::Microsecond);
    CHECK(metrics.curr_deviation < 3 * qts_step + 100 * core::Microsecond);

    LONGS_EQUAL(status::StatusOK, meter.write(new_packet(sn, qts, sts)));
    CHECK(meter.delay_meter()->metrics().curr_deviation < 100 * core::Microsecond);
}

TEST(link_meter, delay_level_stream_timestamp_wrap) {
    packet::FifoQueue queue;
    audio::ArrivalDelayMeterConfig delay_config;
    LinkMeter meter(queue, make_config(), &delay_config, encoding_map, arena, NULL);
    LONGS_EQUAL(status::StatusOK, meter.init_status());

    // Start close to the 32-bit stream timestamp wrap and cross it: the
    // forward-advancing accumulator keeps the level near zero.
    core::nanoseconds_t qts = qts_start;
    packet::stream_timestamp_t sts = (packet::stream_timestamp_t)0 - 50 * sts_step;
    for (packet::seqnum_t sn = 0; sn < 100; sn++) {
        LONGS_EQUAL(status::StatusOK, meter.write(new_packet(sn, qts, sts)));
        qts += qts_step;
        sts += sts_step;
    }

    const audio::ArrivalDelayMetrics& metrics = meter.delay_meter()->metrics();
    CHECK(metrics.curr_deviation < 10 * core::Microsecond);
    CHECK(metrics.baseline > -10 * core::Microsecond);
    CHECK(metrics.baseline < 10 * core::Microsecond);
}

} // namespace rtp
} // namespace roc
