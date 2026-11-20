/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_metrics/atomic_histogram.h
//! @brief Histogram of durations that is observed without taking a lock.

#ifndef ROC_METRICS_ATOMIC_HISTOGRAM_H_
#define ROC_METRICS_ATOMIC_HISTOGRAM_H_

#include "roc_core/noncopyable.h"
#include "roc_core/stddefs.h"
#include "roc_core/time.h"

#include <atomic>
#include <map>
#include <string>

namespace roc {
namespace metrics {

//! Histogram of durations for the audio path.
//!
//! prometheus-cpp's Histogram takes a mutex in Observe(), the same one the
//! scrape thread holds while it reads, so an audio thread that observes can
//! wait for the scrape thread. Here observe() is two atomic additions and
//! never waits. The reader sees each counter at a slightly different moment;
//! it derives the total count from the buckets it read, so the exported
//! count always equals the last bucket, and only the sum can be off by the
//! observations that arrived during the read.
class AtomicHistogram : public core::NonCopyable<> {
public:
    enum {
        //! Largest number of bucket bounds.
        MaxBounds = 32
    };

    //! Initialize with ascending upper bounds of the buckets.
    //! One more bucket collects the values above the last bound.
    AtomicHistogram(const core::nanoseconds_t* bounds, size_t num_bounds);

    //! Count one value. Lock-free; safe to call from any thread.
    void observe(core::nanoseconds_t value);

    //! Number of bucket bounds.
    size_t num_bounds() const;

    //! Upper bound of bucket @p index, for index below num_bounds().
    core::nanoseconds_t bound(size_t index) const;

    //! Values counted in bucket @p index alone, for index up to and
    //! including num_bounds(); the last one is the bucket above every bound.
    uint64_t bucket_count(size_t index) const;

    //! Sum of all counted values.
    core::nanoseconds_t sum() const;

private:
    core::nanoseconds_t bounds_[MaxBounds];
    size_t num_bounds_;
    std::atomic<uint64_t> buckets_[MaxBounds + 1];
    std::atomic<int64_t> sum_;
};

//! Bounds for durations in the audio path, from 10 us to 256 ms. They are
//! dense around the cycle lengths of the audio graph: 1.333 ms is 64 frames
//! at 48 kHz, 2.667 ms is 128 and 5.333 ms is 256.
const core::nanoseconds_t* timing_bounds(size_t& num_bounds);

//! Get the exported histogram with the given name and labels, creating it on
//! the first call. It uses timing_bounds(), lives until the process exits,
//! and is served with the other metrics when the build has Prometheus
//! support. Call it while setting up, not from the audio path: it takes a
//! lock and may allocate.
AtomicHistogram& exported_timing_histogram(
    const char* name, const char* help, const std::map<std::string, std::string>& labels);

} // namespace metrics
} // namespace roc

#endif // ROC_METRICS_ATOMIC_HISTOGRAM_H_
