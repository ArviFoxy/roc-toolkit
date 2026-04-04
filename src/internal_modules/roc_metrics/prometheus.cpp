/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "prometheus.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#endif // ROC_TARGET_PROMETHEUS

#include "roc_core/log.h"
#include <cmath>
#include <unistd.h>
#include <vector>

namespace roc {
namespace metrics {

#ifdef ROC_TARGET_PROMETHEUS

std::vector<double>
generate_logspace_buckets(double min_val, double max_val, int num_buckets) {
    std::vector<double> buckets;
    if (num_buckets <= 0 || min_val <= 0 || max_val <= min_val) {
        return buckets;
    }
    for (int i = 0; i < num_buckets; ++i) {
        double power = static_cast<double>(i) / (num_buckets - 1);
        double val = min_val * std::pow(max_val / min_val, power);
        buckets.push_back(val);
    }
    return buckets;
}

std::shared_ptr<prometheus::Registry> prometheus_registry() {
    static std::shared_ptr<prometheus::Registry> instance =
        std::make_shared<prometheus::Registry>();
    return instance;
}

class PrometheusExporter::Impl {
public:
    Impl(const PrometheusConfig& config) {
        if (config.port > 0) {
            char hostname[256];
            if (gethostname(hostname, sizeof(hostname)) != 0) {
                snprintf(hostname, sizeof(hostname), "unknown");
            }
            roc_log(LogInfo, "prometheus exporter: starting at http://%s:%d/metrics",
                    hostname, config.port);
            std::string bind_address = "0.0.0.0:" + std::to_string(config.port);

            exposer_.reset(new prometheus::Exposer { bind_address });

            // Register the global registry to the exposer
            exposer_->RegisterCollectable(prometheus_registry());
        }
    }

    ~Impl() {
        if (exposer_) {
            roc_log(LogInfo, "prometheus exporter: stopping");
            exposer_.reset();
        }
    }

private:
    std::unique_ptr<prometheus::Exposer> exposer_;
};

#else // ROC_TARGET_PROMETHEUS

// No-op implementation when Prometheus is disabled
class PrometheusExporter::Impl {
public:
    Impl(const PrometheusConfig& config) {
    }
    ~Impl() {
    }
};

#endif // ROC_TARGET_PROMETHEUS

PrometheusExporter::PrometheusExporter(const PrometheusConfig& config)
    : impl_(new Impl(config)) {
}

PrometheusExporter::~PrometheusExporter() = default;

void PrometheusExporter::dispose() {
    this->~PrometheusExporter();
}

} // namespace metrics
} // namespace roc
