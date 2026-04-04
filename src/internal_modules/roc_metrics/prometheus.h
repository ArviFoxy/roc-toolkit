/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file prometheus.h
//! @brief Exposes metrics via Prometheus format over HTTP.

#ifndef ROC_METRICS_PROMETHEUS_H_
#define ROC_METRICS_PROMETHEUS_H_

#include "roc_core/noncopyable.h"
#include "roc_core/time.h"
#include <memory>

#ifdef ROC_TARGET_PROMETHEUS
#include <vector>

namespace prometheus {
class Registry;
}
#endif

namespace roc {
namespace metrics {

//! Configuration for a single Prometheus histogram.
struct HistogramConfig {
    //! Number of logarithmically spaced buckets.
    int buckets;
    //! Minimum bucket boundary, in nanoseconds.
    core::nanoseconds_t min;
    //! Maximum bucket boundary, in nanoseconds.
    core::nanoseconds_t max;

    HistogramConfig(int n_buckets,
                    core::nanoseconds_t min_val,
                    core::nanoseconds_t max_val)
        : buckets(n_buckets)
        , min(min_val)
        , max(max_val) {
    }
};

//! Prometheus metrics configuration.
struct PrometheusConfig {
    int port;
    HistogramConfig niq_latency;
    HistogramConfig e2e_latency;
    HistogramConfig jitter;
    HistogramConfig rtt;

    PrometheusConfig()
        : port(0)
        , niq_latency(100, 5 * core::Millisecond, 50 * core::Millisecond)
        , e2e_latency(100, 20 * core::Millisecond, 200 * core::Millisecond)
        , jitter(100, 100 * core::Microsecond, 200 * core::Millisecond)
        , rtt(100, 1 * core::Millisecond, 100 * core::Millisecond) {
    }
};

#ifdef ROC_TARGET_PROMETHEUS
//! Global registry for internal components to register their metrics.
std::shared_ptr<prometheus::Registry> prometheus_registry();

//! Generate logarithmically spaced histogram bucket boundaries (in seconds)
//! from a HistogramConfig whose min/max are in nanoseconds.
std::vector<double> generate_logspace_buckets(const HistogramConfig& config);
#endif // ROC_TARGET_PROMETHEUS

//! Exposes native Prometheus metrics on an HTTP endpoint.
//!
//! This component runs a background thread that listens on a port to serve
//! HTTP metrics. It implements a no-op stub pattern if Prometheus is disabled.
class PrometheusExporter : public core::NonCopyable<> {
public:
    //! Start exporting on the configured port.
    PrometheusExporter(const PrometheusConfig& config);

    //! Stop the exporter and clean up resources.
    ~PrometheusExporter();

    //! Invoked by allocators like core::ScopedPtr.
    void dispose();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace metrics
} // namespace roc

#endif // ROC_METRICS_PROMETHEUS_H_
