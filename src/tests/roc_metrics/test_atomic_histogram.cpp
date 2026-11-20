/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "test_harness.h"

#include "roc_core/thread.h"
#include "roc_metrics/atomic_histogram.h"
#include "roc_metrics/exported_histograms.h"

namespace roc {
namespace metrics {

namespace {

const core::nanoseconds_t Bounds[] = { 10, 20, 40 };

class Observer : public core::Thread {
public:
    Observer(AtomicHistogram& hist, core::nanoseconds_t value, size_t times)
        : hist_(hist)
        , value_(value)
        , times_(times) {
    }

private:
    virtual void run() {
        for (size_t i = 0; i < times_; i++) {
            hist_.observe(value_);
        }
    }

    AtomicHistogram& hist_;
    const core::nanoseconds_t value_;
    const size_t times_;
};

} // namespace

TEST_GROUP(atomic_histogram) { };

TEST(atomic_histogram, value_on_bound_counts_in_that_bucket) {
    AtomicHistogram hist(Bounds, 3);

    hist.observe(10); // on the first bound
    hist.observe(11); // just above it
    hist.observe(20);
    hist.observe(41); // above every bound
    hist.observe(-5); // below zero: first bucket

    UNSIGNED_LONGS_EQUAL(3, hist.num_bounds());
    UNSIGNED_LONGS_EQUAL(2, hist.bucket_count(0));
    UNSIGNED_LONGS_EQUAL(2, hist.bucket_count(1));
    UNSIGNED_LONGS_EQUAL(0, hist.bucket_count(2));
    UNSIGNED_LONGS_EQUAL(1, hist.bucket_count(3));
    LONGS_EQUAL(10 + 11 + 20 + 41 - 5, hist.sum());
}

TEST(atomic_histogram, concurrent_observations_are_all_counted) {
    enum { Times = 200000 };

    AtomicHistogram hist(Bounds, 3);

    Observer a(hist, 5, Times);
    Observer b(hist, 15, Times);
    Observer c(hist, 100, Times);

    CHECK(a.start());
    CHECK(b.start());
    CHECK(c.start());

    a.join();
    b.join();
    c.join();

    UNSIGNED_LONGS_EQUAL(Times, hist.bucket_count(0));
    UNSIGNED_LONGS_EQUAL(Times, hist.bucket_count(1));
    UNSIGNED_LONGS_EQUAL(0, hist.bucket_count(2));
    UNSIGNED_LONGS_EQUAL(Times, hist.bucket_count(3));
    LONGS_EQUAL((core::nanoseconds_t)Times * (5 + 15 + 100), hist.sum());
}

TEST(atomic_histogram, timing_bounds_are_ascending_and_hold_the_cycle_lengths) {
    size_t num_bounds = 0;
    const core::nanoseconds_t* bounds = timing_bounds(num_bounds);

    CHECK(num_bounds > 0);
    CHECK(num_bounds <= (size_t)AtomicHistogram::MaxBounds);

    bool has_64 = false, has_128 = false, has_256 = false;
    for (size_t i = 0; i < num_bounds; i++) {
        if (i > 0) {
            CHECK(bounds[i] > bounds[i - 1]);
        }
        has_64 = has_64 || bounds[i] == 1333 * core::Microsecond;
        has_128 = has_128 || bounds[i] == 2667 * core::Microsecond;
        has_256 = has_256 || bounds[i] == 5333 * core::Microsecond;
    }
    CHECK(has_64 && has_128 && has_256);
}

TEST(atomic_histogram, same_name_and_labels_give_the_same_histogram) {
    std::map<std::string, std::string> sink, source;
    sink["device"] = "sink";
    source["device"] = "source";

    AtomicHistogram& a = exported_timing_histogram("test_hist_seconds", "help", sink);
    AtomicHistogram& b = exported_timing_histogram("test_hist_seconds", "help", sink);
    AtomicHistogram& c = exported_timing_histogram("test_hist_seconds", "help", source);

    POINTERS_EQUAL(&a, &b);
    CHECK(&a != &c);

    size_t found = 0;
    const std::vector<ExportedHistogram> list = exported_histograms();
    for (size_t i = 0; i < list.size(); i++) {
        if (list[i].name == "test_hist_seconds") {
            found++;
        }
    }
    UNSIGNED_LONGS_EQUAL(2, found);
}

} // namespace metrics
} // namespace roc
