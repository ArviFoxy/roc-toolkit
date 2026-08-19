/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_packet/capture_timestamp_mapping.h"

namespace roc {
namespace packet {

namespace {

enum { SampleRate = 1000 };

const core::nanoseconds_t NsPerSample = core::Second / SampleRate;

const audio::SampleSpec sample_spec(SampleRate,
                                    audio::PcmSubformat_Raw,
                                    audio::ChanLayout_Surround,
                                    audio::ChanOrder_Smpte,
                                    audio::ChanMask_Surround_Stereo);

const core::nanoseconds_t CtsBase = core::Second * 1000;

} // namespace

TEST_GROUP(capture_timestamp_mapping) {};

TEST(capture_timestamp_mapping, initial_state) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(!mapping.has_mapping());
    LONGLONGS_EQUAL(0, mapping.base_capture_ts());
    UNSIGNED_LONGS_EQUAL(0, mapping.base_stream_ts());
}

TEST(capture_timestamp_mapping, update) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(mapping.update(CtsBase, 500));

    CHECK(mapping.has_mapping());
    LONGLONGS_EQUAL(CtsBase, mapping.base_capture_ts());
    UNSIGNED_LONGS_EQUAL(500, mapping.base_stream_ts());
}

TEST(capture_timestamp_mapping, update_latest_wins) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(mapping.update(CtsBase, 500));
    CHECK(mapping.update(CtsBase + core::Second, 700));

    LONGLONGS_EQUAL(CtsBase + core::Second, mapping.base_capture_ts());
    UNSIGNED_LONGS_EQUAL(700, mapping.base_stream_ts());
}

TEST(capture_timestamp_mapping, update_rejects_non_positive_cts) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(!mapping.update(0, 500));
    CHECK(!mapping.update(-CtsBase, 500));
    CHECK(!mapping.has_mapping());

    // A rejected pair keeps the previous one.
    CHECK(mapping.update(CtsBase, 500));
    CHECK(!mapping.update(0, 900));

    CHECK(mapping.has_mapping());
    LONGLONGS_EQUAL(CtsBase, mapping.base_capture_ts());
    UNSIGNED_LONGS_EQUAL(500, mapping.base_stream_ts());
}

TEST(capture_timestamp_mapping, reset) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(mapping.update(CtsBase, 500));
    mapping.reset();

    CHECK(!mapping.has_mapping());
    LONGLONGS_EQUAL(0, mapping.base_capture_ts());
    UNSIGNED_LONGS_EQUAL(0, mapping.base_stream_ts());
}

TEST(capture_timestamp_mapping, position_to_capture_ts) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(mapping.update(CtsBase, 500));

    LONGLONGS_EQUAL(CtsBase, mapping.capture_ts(500));
    LONGLONGS_EQUAL(CtsBase + 100 * NsPerSample, mapping.capture_ts(600));
    LONGLONGS_EQUAL(CtsBase - 300 * NsPerSample, mapping.capture_ts(200));
}

TEST(capture_timestamp_mapping, capture_ts_to_position) {
    CaptureTimestampMapping mapping(sample_spec);

    CHECK(mapping.update(CtsBase, 500));

    UNSIGNED_LONGS_EQUAL(500, mapping.position(CtsBase));
    UNSIGNED_LONGS_EQUAL(600, mapping.position(CtsBase + 100 * NsPerSample));
    UNSIGNED_LONGS_EQUAL(200, mapping.position(CtsBase - 300 * NsPerSample));
}

TEST(capture_timestamp_mapping, wraparound) {
    CaptureTimestampMapping mapping(sample_spec);

    const stream_timestamp_t near_wrap = (stream_timestamp_t)-100;

    CHECK(mapping.update(CtsBase, near_wrap));

    // Position past the 32-bit wrap point is 300 samples after the pair.
    const stream_timestamp_t wrapped = near_wrap + 300;
    UNSIGNED_LONGS_EQUAL(200, wrapped);
    LONGLONGS_EQUAL(CtsBase + 300 * NsPerSample, mapping.capture_ts(wrapped));

    // And the reverse conversion wraps the same way.
    UNSIGNED_LONGS_EQUAL(wrapped, mapping.position(CtsBase + 300 * NsPerSample));
}

} // namespace packet
} // namespace roc
