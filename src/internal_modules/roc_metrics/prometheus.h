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

//! Prometheus metrics configuration.
struct PrometheusConfig {
    int port;
    int latency_buckets;
    core::nanoseconds_t latency_min;
    core::nanoseconds_t latency_max;
    int jitter_buckets;
    core::nanoseconds_t jitter_min;
    core::nanoseconds_t jitter_max;
    int rtt_buckets;
    core::nanoseconds_t rtt_min;
    core::nanoseconds_t rtt_max;

    PrometheusConfig()
        : port(0)
        , latency_buckets(32)
        , latency_min(1 * core::Millisecond)
        , latency_max(1000 * core::Millisecond)
        , jitter_buckets(32)
        , jitter_min(100 * core::Microsecond)
        , jitter_max(200 * core::Millisecond)
        , rtt_buckets(32)
        , rtt_min(100 * core::Microsecond)
        , rtt_max(200 * core::Millisecond) {
    }
};

#ifdef ROC_TARGET_PROMETHEUS
//! Global registry for internal components to register their metrics.
std::shared_ptr<prometheus::Registry> prometheus_registry();

//! Generate logarithmically spaced histogram bucket boundaries.
std::vector<double>
generate_logspace_buckets(double min_val, double max_val, int num_buckets);
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
