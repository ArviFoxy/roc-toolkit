/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_metrics/atomic_histogram.h"
#include "roc_core/macro_helpers.h"
#include "roc_core/panic.h"
#include "roc_metrics/exported_histograms.h"

#include <mutex>

namespace roc {
namespace metrics {

namespace {

const core::nanoseconds_t TimingBounds[] = {
    10 * core::Microsecond,   20 * core::Microsecond,   50 * core::Microsecond,
    100 * core::Microsecond,  200 * core::Microsecond,  350 * core::Microsecond,
    500 * core::Microsecond,  750 * core::Microsecond,  1 * core::Millisecond,
    1333 * core::Microsecond, 2 * core::Millisecond,    2667 * core::Microsecond,
    4 * core::Millisecond,    5333 * core::Microsecond, 8 * core::Millisecond,
    16 * core::Millisecond,   32 * core::Millisecond,   64 * core::Millisecond,
    128 * core::Millisecond,  256 * core::Millisecond,
};

std::mutex& exported_mutex() {
    static std::mutex mutex;
    return mutex;
}

// Never destroyed: audio threads may still observe while the process exits.
std::vector<ExportedHistogram>& exported_list() {
    static std::vector<ExportedHistogram>* list = new std::vector<ExportedHistogram>();
    return *list;
}

} // namespace

AtomicHistogram::AtomicHistogram(const core::nanoseconds_t* bounds, size_t num_bounds)
    : num_bounds_(num_bounds)
    , sum_(0) {
    if (num_bounds == 0 || num_bounds > MaxBounds) {
        roc_panic("atomic histogram: number of bounds out of range: %lu",
                  (unsigned long)num_bounds);
    }
    for (size_t i = 0; i < num_bounds; i++) {
        if (i > 0 && bounds[i] <= bounds[i - 1]) {
            roc_panic("atomic histogram: bounds are not ascending");
        }
        bounds_[i] = bounds[i];
    }
    for (size_t i = 0; i <= MaxBounds; i++) {
        buckets_[i].store(0, std::memory_order_relaxed);
    }
}

void AtomicHistogram::observe(core::nanoseconds_t value) {
    // A linear scan: there are at most 32 bounds, and the values of
    // interest fall in the first few.
    size_t index = 0;
    while (index < num_bounds_ && value > bounds_[index]) {
        index++;
    }
    buckets_[index].fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(value, std::memory_order_relaxed);
}

size_t AtomicHistogram::num_bounds() const {
    return num_bounds_;
}

core::nanoseconds_t AtomicHistogram::bound(size_t index) const {
    roc_panic_if(index >= num_bounds_);
    return bounds_[index];
}

uint64_t AtomicHistogram::bucket_count(size_t index) const {
    roc_panic_if(index > num_bounds_);
    return buckets_[index].load(std::memory_order_relaxed);
}

core::nanoseconds_t AtomicHistogram::sum() const {
    return sum_.load(std::memory_order_relaxed);
}

const core::nanoseconds_t* timing_bounds(size_t& num_bounds) {
    num_bounds = ROC_ARRAY_SIZE(TimingBounds);
    return TimingBounds;
}

AtomicHistogram&
exported_timing_histogram(const char* name,
                          const char* help,
                          const std::map<std::string, std::string>& labels) {
    std::lock_guard<std::mutex> lock(exported_mutex());

    std::vector<ExportedHistogram>& list = exported_list();
    for (size_t i = 0; i < list.size(); i++) {
        if (list[i].name == name && list[i].labels == labels) {
            return *list[i].histogram;
        }
    }

    size_t num_bounds = 0;
    const core::nanoseconds_t* bounds = timing_bounds(num_bounds);

    ExportedHistogram exported;
    exported.name = name;
    exported.help = help;
    exported.labels = labels;
    exported.histogram = new AtomicHistogram(bounds, num_bounds);
    list.push_back(exported);

    return *exported.histogram;
}

std::vector<ExportedHistogram> exported_histograms() {
    std::lock_guard<std::mutex> lock(exported_mutex());
    return exported_list();
}

} // namespace metrics
} // namespace roc
