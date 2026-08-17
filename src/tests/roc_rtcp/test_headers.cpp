/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_packet/ntp.h"
#include "roc_rtcp/headers.h"

namespace roc {
namespace rtcp {

TEST_GROUP(headers) {};

TEST(headers, bit_fields) {
    { // get_bit_field
        uint32_t val = 0xAABBCCDD;

        CHECK_EQUAL(0xAA, header::get_bit_field(val, 24, 0xffff));
        CHECK_EQUAL(0xAAB, header::get_bit_field(val, 20, 0xffff));
        CHECK_EQUAL(0xAABB, header::get_bit_field(val, 16, 0xffff));
        CHECK_EQUAL(0xABB, header::get_bit_field(val, 16, 0xfff));

        CHECK_EQUAL(0xAABBCCDD, header::get_bit_field(val, 0, 0xffffffff));
        CHECK_EQUAL(0, header::get_bit_field(val, 0, 0));
    }
    { // set_bit_field
        uint32_t val = 0;

        header::set_bit_field(val, (uint32_t)0xDD, 4, 0xf);
        CHECK_EQUAL(0xD0, val);

        header::set_bit_field(val, (uint32_t)0xC, 8, 0xf);
        CHECK_EQUAL(0xCD0, val);

        header::set_bit_field(val, (uint32_t)0xE, 4, 0xf);
        CHECK_EQUAL(0xCE0, val);
    }
}

TEST(headers, losses) {
    { // fract_loss
        header::ReceptionReportBlock blk;

        blk.set_fract_loss(0.25f);
        DOUBLES_EQUAL(0.25, blk.fract_loss(), 0.01);

        blk.set_fract_loss(0.75f);
        DOUBLES_EQUAL(0.75, blk.fract_loss(), 0.01);

        blk.set_fract_loss(0.0f);
        DOUBLES_EQUAL(0.0, blk.fract_loss(), 0.01);

        blk.set_fract_loss(1.0f);
        DOUBLES_EQUAL(1.0, blk.fract_loss(), 0.01);
        CHECK(blk.fract_loss() <= 1.0f);

        blk.set_fract_loss(-0.25f);
        DOUBLES_EQUAL(0.0, blk.fract_loss(), 0.01);

        blk.set_fract_loss(1.25f);
        DOUBLES_EQUAL(1.0, blk.fract_loss(), 0.01);
        CHECK(blk.fract_loss() <= 1.0f);
    }
    { // cum_loss
        header::ReceptionReportBlock blk;

        blk.set_cum_loss(100);
        CHECK_EQUAL(100, blk.cum_loss());

        blk.set_cum_loss(-100);
        CHECK_EQUAL(-100, blk.cum_loss());

        blk.set_cum_loss(0);
        CHECK_EQUAL(0, blk.cum_loss());

        blk.set_cum_loss(8388607);
        CHECK_EQUAL(8388607, blk.cum_loss());

        blk.set_cum_loss(-8388608);
        CHECK_EQUAL(-8388608, blk.cum_loss());

        blk.set_cum_loss(8388608);
        CHECK_EQUAL(8388607, blk.cum_loss());

        blk.set_cum_loss(-8388609);
        CHECK_EQUAL(-8388608, blk.cum_loss());

        blk.set_cum_loss(99999999);
        CHECK_EQUAL(8388607, blk.cum_loss());

        blk.set_cum_loss(-99999999);
        CHECK_EQUAL(-8388608, blk.cum_loss());
    }
}

TEST(headers, timestamps) {
    { // SR
        header::SenderReportPacket blk;

        blk.set_ntp_timestamp(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.ntp_timestamp());

        blk.set_ntp_timestamp(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD1111, blk.ntp_timestamp());

        blk.set_ntp_timestamp(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDD8888, blk.ntp_timestamp());

        blk.set_ntp_timestamp(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x1111AABBCCDD0000, blk.ntp_timestamp());
    }
    { // RRTR
        header::XrRrtrBlock blk;

        blk.set_ntp_timestamp(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.ntp_timestamp());

        blk.set_ntp_timestamp(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD1111, blk.ntp_timestamp());

        blk.set_ntp_timestamp(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDD8888, blk.ntp_timestamp());

        blk.set_ntp_timestamp(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x1111AABBCCDD0000, blk.ntp_timestamp());
    }
    { // LSR
        header::ReceptionReportBlock blk;

        blk.set_last_sr(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_sr());

        blk.set_last_sr(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_sr());

        blk.set_last_sr(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_sr());

        blk.set_last_sr(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_sr());
    }
    { // DLSR
        header::ReceptionReportBlock blk;

        blk.set_delay_last_sr(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.delay_last_sr());

        blk.set_delay_last_sr(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.delay_last_sr());

        blk.set_delay_last_sr(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.delay_last_sr());

        blk.set_delay_last_sr(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.delay_last_sr());
    }
    { // LRR
        header::XrDlrrSubblock blk;

        blk.set_last_rr(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_rr());

        blk.set_last_rr(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_rr());

        blk.set_last_rr(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_rr());

        blk.set_last_rr(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.last_rr());
    }
    { // DLRR
        header::XrDlrrSubblock blk;

        blk.set_delay_last_rr(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.delay_last_rr());

        blk.set_delay_last_rr(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.delay_last_rr());

        blk.set_delay_last_rr(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.delay_last_rr());

        blk.set_delay_last_rr(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.delay_last_rr());
    }
}

TEST(headers, intervals) {
    { // interval_duration
        header::XrMeasurementInfoBlock blk;

        blk.set_interval_duration(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.interval_duration());

        blk.set_interval_duration(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.interval_duration());

        blk.set_interval_duration(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.interval_duration());

        blk.set_interval_duration(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.interval_duration());
    }
    { // cumulative_duration
        header::XrMeasurementInfoBlock blk;

        blk.set_cum_duration(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.cum_duration());

        blk.set_cum_duration(0x0000AABBCCDD1111);
        CHECK_EQUAL(0x0000AABBCCDD1111, blk.cum_duration());

        blk.set_cum_duration(0x0000AABBCCDD8888);
        CHECK_EQUAL(0x0000AABBCCDD8888, blk.cum_duration());

        blk.set_cum_duration(0x1111AABBCCDD0000);
        CHECK_EQUAL(0x1111AABBCCDD0000, blk.cum_duration());
    }
}

TEST(headers, metrics) {
    { // mean_rtt
        header::XrDelayMetricsBlock blk;

        CHECK(!blk.has_mean_rtt());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.mean_rtt());

        blk.set_mean_rtt(0x0000AABBCCDD0000);
        CHECK(blk.has_mean_rtt());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.mean_rtt());

        blk.set_mean_rtt(0x0000AABBCCDD1111);
        CHECK(blk.has_mean_rtt());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.mean_rtt());

        blk.set_mean_rtt(0x0000AABBCCDD8888);
        CHECK(blk.has_mean_rtt());
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.mean_rtt());

        blk.set_mean_rtt(0x1111AABBCCDD0000);
        CHECK(blk.has_mean_rtt());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.mean_rtt());

        blk.set_mean_rtt(0x0000FFFFFFFE8000);
        CHECK(blk.has_mean_rtt());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.mean_rtt());

        blk.reset();

        CHECK(!blk.has_mean_rtt());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.mean_rtt());
    }
    { // min_rtt
        header::XrDelayMetricsBlock blk;

        CHECK(!blk.has_min_rtt());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.min_rtt());

        blk.set_min_rtt(0x0000AABBCCDD0000);
        CHECK(blk.has_min_rtt());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.min_rtt());

        blk.set_min_rtt(0x0000AABBCCDD1111);
        CHECK(blk.has_min_rtt());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.min_rtt());

        blk.set_min_rtt(0x0000AABBCCDD8888);
        CHECK(blk.has_min_rtt());
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.min_rtt());

        blk.set_min_rtt(0x1111AABBCCDD0000);
        CHECK(blk.has_min_rtt());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.min_rtt());

        blk.set_min_rtt(0x0000FFFFFFFE8000);
        CHECK(blk.has_min_rtt());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.min_rtt());

        blk.reset();

        CHECK(!blk.has_min_rtt());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.min_rtt());
    }
    { // max_rtt
        header::XrDelayMetricsBlock blk;

        CHECK(!blk.has_max_rtt());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.max_rtt());

        blk.set_max_rtt(0x0000AABBCCDD0000);
        CHECK(blk.has_max_rtt());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.max_rtt());

        blk.set_max_rtt(0x0000AABBCCDD1111);
        CHECK(blk.has_max_rtt());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.max_rtt());

        blk.set_max_rtt(0x0000AABBCCDD8888);
        CHECK(blk.has_max_rtt());
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.max_rtt());

        blk.set_max_rtt(0x1111AABBCCDD0000);
        CHECK(blk.has_max_rtt());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.max_rtt());

        blk.set_max_rtt(0x0000FFFFFFFE8000);
        CHECK(blk.has_max_rtt());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.max_rtt());

        blk.reset();

        CHECK(!blk.has_max_rtt());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.max_rtt());
    }
    { // e2e_latency
        header::XrDelayMetricsBlock blk;

        CHECK(!blk.has_e2e_latency());
        CHECK_EQUAL(0xFFFFFFFFFFFFFFFF, blk.e2e_latency());

        blk.set_e2e_latency(0x0000AABBCCDD0000);
        CHECK(blk.has_e2e_latency());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.e2e_latency());

        blk.set_e2e_latency(0x0000AABBCCDD1111);
        CHECK(blk.has_e2e_latency());
        CHECK_EQUAL(0x0000AABBCCDD1111, blk.e2e_latency());

        blk.set_e2e_latency(0x1111AABBCCDD0000);
        CHECK(blk.has_e2e_latency());
        CHECK_EQUAL(0x1111AABBCCDD0000, blk.e2e_latency());

        blk.set_e2e_latency(0xFFFFFFFFFFFFFFFF);
        CHECK(blk.has_e2e_latency());
        CHECK_EQUAL(0xFFFFFFFFFFFFFFFE, blk.e2e_latency());

        blk.reset();

        CHECK(!blk.has_e2e_latency());
        CHECK_EQUAL(0xFFFFFFFFFFFFFFFF, blk.e2e_latency());
    }
    { // niq_latency
        header::XrQueueMetricsBlock blk;

        CHECK(!blk.has_niq_latency());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.niq_latency());

        blk.set_niq_latency(0x0000AABBCCDD0000);
        CHECK(blk.has_niq_latency());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.niq_latency());

        blk.set_niq_latency(0x0000AABBCCDD1111);
        CHECK(blk.has_niq_latency());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.niq_latency());

        blk.set_niq_latency(0x0000AABBCCDD8888);
        CHECK(blk.has_niq_latency());
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.niq_latency());

        blk.set_niq_latency(0x1111AABBCCDD0000);
        CHECK(blk.has_niq_latency());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.niq_latency());

        blk.set_niq_latency(0x0000FFFFFFFE8000);
        CHECK(blk.has_niq_latency());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.niq_latency());

        blk.reset();

        CHECK(!blk.has_niq_latency());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.niq_latency());
    }
    { // niq_stalling
        header::XrQueueMetricsBlock blk;

        CHECK(!blk.has_niq_stalling());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.niq_stalling());

        blk.set_niq_stalling(0x0000AABBCCDD0000);
        CHECK(blk.has_niq_stalling());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.niq_stalling());

        blk.set_niq_stalling(0x0000AABBCCDD1111);
        CHECK(blk.has_niq_stalling());
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.niq_stalling());

        blk.set_niq_stalling(0x0000AABBCCDD8888);
        CHECK(blk.has_niq_stalling());
        CHECK_EQUAL(0x0000AABBCCDE0000, blk.niq_stalling());

        blk.set_niq_stalling(0x1111AABBCCDD0000);
        CHECK(blk.has_niq_stalling());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.niq_stalling());

        blk.set_niq_stalling(0x0000FFFFFFFE8000);
        CHECK(blk.has_niq_stalling());
        CHECK_EQUAL(0x0000FFFFFFFE0000, blk.niq_stalling());

        blk.reset();

        CHECK(!blk.has_niq_stalling());
        CHECK_EQUAL(0x0000FFFFFFFF0000, blk.niq_stalling());
    }
}

TEST(headers, stream_snapshot) {
    { // block sizes are wire-format constants
        CHECK_EQUAL(16, sizeof(header::XrStreamSnapshotBlock));
        CHECK_EQUAL(36, sizeof(header::XrStreamSnapshotEntry));
        CHECK_EQUAL(9, header::XrStreamSnapshotBlock::EntryWords);
    }
    { // fixed part defaults and fields
        header::XrStreamSnapshotBlock blk;

        CHECK_EQUAL(header::XR_STREAM_SNAPSHOT, blk.header().block_type());
        CHECK_EQUAL(header::XrStreamSnapshotBlock::Version, blk.version());
        CHECK_EQUAL(header::XrStreamSnapshotBlock::EntryWords, blk.entry_words());
        CHECK_EQUAL(0, blk.n_entries());
        CHECK_EQUAL(0, blk.ssrc());

        blk.set_ssrc(0xAABBCCDD);
        CHECK_EQUAL(0xAABBCCDD, blk.ssrc());

        blk.set_grid_period(0x0000AABBCCDD0000);
        CHECK_EQUAL(0x0000AABBCCDD0000, blk.grid_period());

        blk.set_version(3);
        CHECK_EQUAL(3, blk.version());
        // Version nibble must not clobber the block type.
        CHECK_EQUAL(header::XR_STREAM_SNAPSHOT, blk.header().block_type());

        blk.set_n_entries(4);
        CHECK_EQUAL(4, blk.n_entries());

        blk.reset();
        CHECK_EQUAL(header::XrStreamSnapshotBlock::Version, blk.version());
        CHECK_EQUAL(0, blk.n_entries());
    }
    { // version readable from a bare block header
        header::XrStreamSnapshotBlock blk;
        blk.set_version(2);
        CHECK_EQUAL(2, header::XrStreamSnapshotBlock::version_of(blk.header()));
    }
    { // entry: plain fields
        header::XrStreamSnapshotEntry e;

        CHECK_EQUAL(0, e.grid_index());
        CHECK_EQUAL(0, e.position());

        e.set_grid_index(0xDEADBEEF);
        CHECK_EQUAL(0xDEADBEEF, e.grid_index());

        e.set_position(0x11223344);
        CHECK_EQUAL(0x11223344, e.position());
    }
    { // entry: NTP32 metrics with sentinel + clamping (same rules as
      // queue metrics: truncation to middle 32 bits, rounding, saturation)
        header::XrStreamSnapshotEntry e;

        CHECK(!e.has_niq_instant());
        CHECK(!e.has_niq_mean());
        CHECK(!e.has_e2e_latency());
        CHECK(!e.has_target_latency());

        e.set_niq_instant(0x0000AABBCCDD0000);
        CHECK(e.has_niq_instant());
        CHECK_EQUAL(0x0000AABBCCDD0000, e.niq_instant());

        e.set_niq_mean(0x0000AABBCCDD8888);
        CHECK(e.has_niq_mean());
        CHECK_EQUAL(0x0000AABBCCDE0000, e.niq_mean());

        e.set_e2e_latency(0x1111AABBCCDD0000);
        CHECK(e.has_e2e_latency());
        CHECK_EQUAL(0x0000FFFFFFFE0000, e.e2e_latency());

        e.set_target_latency(0x0000AABBCCDD0000);
        CHECK(e.has_target_latency());
        CHECK_EQUAL(0x0000AABBCCDD0000, e.target_latency());

        e.reset();
        CHECK(!e.has_niq_instant());
        CHECK(!e.has_niq_mean());
        CHECK(!e.has_e2e_latency());
        CHECK(!e.has_target_latency());
    }
    { // entry: signed warp with sentinel
        header::XrStreamSnapshotEntry e;

        CHECK(!e.has_warp());

        e.set_warp_ppb(0);
        CHECK(e.has_warp());
        CHECK_EQUAL(0, e.warp_ppb());

        e.set_warp_ppb(-123456789);
        CHECK(e.has_warp());
        CHECK_EQUAL(-123456789, e.warp_ppb());

        e.set_warp_ppb(2000000000);
        CHECK(e.has_warp());
        CHECK_EQUAL(2000000000, e.warp_ppb());

        e.reset();
        CHECK(!e.has_warp());
    }
    { // entry: NTP64 local time with sentinel
        header::XrStreamSnapshotEntry e;

        CHECK(!e.has_recv_local_time());

        e.set_recv_local_time(0x1122334455667788);
        CHECK(e.has_recv_local_time());
        CHECK_EQUAL(0x1122334455667788, e.recv_local_time());

        e.reset();
        CHECK(!e.has_recv_local_time());
    }
}

} // namespace rtcp
} // namespace roc
