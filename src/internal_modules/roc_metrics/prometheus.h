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
#include <map>
#include <string>
#include <vector>

namespace prometheus {
class Registry;
}
#endif

namespace roc {
namespace metrics {

//! Histogram bucket spacing mode.
enum HistogramScale {
    //! Logarithmically spaced buckets.
    HistogramScale_Log,
    //! Linearly spaced buckets.
    HistogramScale_Linear
};

//! Configuration for a single Prometheus histogram.
struct HistogramConfig {
    //! Number of buckets.
    int buckets;
    //! Minimum bucket boundary, in nanoseconds.
    core::nanoseconds_t min;
    //! Maximum bucket boundary, in nanoseconds.
    core::nanoseconds_t max;
    //! Bucket spacing mode.
    HistogramScale scale;

    HistogramConfig(int n_buckets,
                    core::nanoseconds_t min_val,
                    core::nanoseconds_t max_val,
                    HistogramScale scale_val = HistogramScale_Log)
        : buckets(n_buckets)
        , min(min_val)
        , max(max_val)
        , scale(scale_val) {
    }
};

//! Identity attached to a component's exported metrics: which peer side the
//! shared components' metric names should use, and which sender slot (if
//! any) the series belongs to.
struct MetricsScope {
    //! Which name prefix the shared components use.
    enum Side {
        //! roc_recv_* names (the default; receiver pipelines).
        Side_Recv = 0,
        //! roc_send_* names (sender pipelines).
        Side_Send
    };

    //! Side of the peer exporting the metric.
    Side side;

    //! Value of the "slot" label attached to the series.
    //! Empty string (default) = no label, one unlabeled series per process.
    char slot[64];

    MetricsScope()
        : side(Side_Recv) {
        slot[0] = '\0';
    }

    //! Set slot label value (truncated to the buffer size).
    void set_slot(const char* value) {
        size_t n = 0;
        if (value) {
            while (value[n] != '\0' && n < sizeof(slot) - 1) {
                slot[n] = value[n];
                n++;
            }
        }
        slot[n] = '\0';
    }
};

//! Prometheus metrics configuration.
struct PrometheusConfig {
    int port;
    MetricsScope scope;
    HistogramConfig niq_latency;
    HistogramConfig e2e_latency;
    HistogramConfig jitter;
    HistogramConfig rtt;
    HistogramConfig playout_spread;
    HistogramConfig playout_fleet_mean;

    // playout_spread bounds also serve the fleet stddev histogram:
    // both statistics live in the same sub-millisecond decade, with the
    // log midpoint near the observed modes (stddev ~0.3ms, spread
    // ~0.8ms). The fleet mean sits near the target latency, two
    // decades higher, so it has its own bounds (log midpoint ~22ms).
    PrometheusConfig()
        : port(0)
        , niq_latency(100, 5 * core::Millisecond, 50 * core::Millisecond)
        , e2e_latency(100, 20 * core::Millisecond, 200 * core::Millisecond)
        , jitter(100, 100 * core::Microsecond, 200 * core::Millisecond)
        , rtt(100, 1 * core::Millisecond, 100 * core::Millisecond)
        , playout_spread(40, 10 * core::Microsecond, 10 * core::Millisecond)
        , playout_fleet_mean(40, 5 * core::Millisecond, 100 * core::Millisecond) {
    }
};

#ifdef ROC_TARGET_PROMETHEUS
//! Global registry for internal components to register their metrics.
std::shared_ptr<prometheus::Registry> prometheus_registry();

//! Label set for a scope: empty when scope.slot is empty, otherwise
//! {{"slot", scope.slot}}.
std::map<std::string, std::string> scope_labels(const MetricsScope& scope);

//! Build Prometheus labels for a slot pair (slot_a / slot_b).
std::map<std::string, std::string> pair_labels(const char* slot_a, const char* slot_b);

//! Metric name for a scope: "roc_send_" or "roc_recv_" + suffix,
//! depending on scope.side.
std::string scope_metric_name(const MetricsScope& scope, const char* suffix);

//! Generate histogram bucket boundaries (in seconds)
//! from a HistogramConfig whose min/max are in nanoseconds.
//! Uses logarithmic or linear spacing based on config.scale.
std::vector<double> generate_histogram_buckets(const HistogramConfig& config);
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
