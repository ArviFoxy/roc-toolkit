/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "prometheus.h"

#ifdef ROC_TARGET_PROMETHEUS
#include "roc_metrics/exported_histograms.h"

#include <limits>
#include <prometheus/collectable.h>
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

std::vector<double> generate_histogram_buckets(const HistogramConfig& config) {
    const double min_val = (double)config.min / 1e9;
    const double max_val = (double)config.max / 1e9;
    const int num_buckets = config.buckets;

    std::vector<double> buckets;
    if (num_buckets <= 0 || min_val <= 0 || max_val <= min_val) {
        return buckets;
    }
    for (int i = 0; i < num_buckets; ++i) {
        double val;
        if (config.scale == HistogramScale_Linear) {
            val = min_val
                + (max_val - min_val) * static_cast<double>(i) / (num_buckets - 1);
        } else {
            double power = static_cast<double>(i) / (num_buckets - 1);
            val = min_val * std::pow(max_val / min_val, power);
        }
        buckets.push_back(val);
    }
    return buckets;
}

std::shared_ptr<prometheus::Registry> prometheus_registry() {
    static std::shared_ptr<prometheus::Registry> instance =
        std::make_shared<prometheus::Registry>();
    return instance;
}

std::map<std::string, std::string> scope_labels(const MetricsScope& scope) {
    std::map<std::string, std::string> labels;
    if (scope.slot[0] != '\0') {
        labels["slot"] = scope.slot;
    }
    return labels;
}

std::map<std::string, std::string> pair_labels(const char* slot_a, const char* slot_b) {
    std::map<std::string, std::string> labels;
    labels["slot_a"] = slot_a;
    labels["slot_b"] = slot_b;
    return labels;
}

std::string scope_metric_name(const MetricsScope& scope, const char* suffix) {
    std::string name(scope.side == MetricsScope::Side_Send ? "roc_send_" : "roc_recv_");
    name += suffix;
    return name;
}

namespace {

// Serves the histograms created by exported_timing_histogram(). Series of
// the same name become one family, as the text format requires.
class AtomicHistogramCollector : public prometheus::Collectable {
public:
    std::vector<prometheus::MetricFamily> Collect() const override {
        std::vector<prometheus::MetricFamily> families;

        const std::vector<ExportedHistogram> exported = exported_histograms();

        for (size_t n = 0; n < exported.size(); n++) {
            const ExportedHistogram& src = exported[n];

            prometheus::MetricFamily* family = NULL;
            for (size_t f = 0; f < families.size(); f++) {
                if (families[f].name == src.name) {
                    family = &families[f];
                }
            }
            if (!family) {
                families.push_back(prometheus::MetricFamily());
                family = &families.back();
                family->name = src.name;
                family->help = src.help;
                family->type = prometheus::MetricType::Histogram;
            }

            prometheus::ClientMetric metric;
            for (std::map<std::string, std::string>::const_iterator it =
                     src.labels.begin();
                 it != src.labels.end(); ++it) {
                prometheus::ClientMetric::Label label;
                label.name = it->first;
                label.value = it->second;
                metric.label.push_back(label);
            }

            const AtomicHistogram& hist = *src.histogram;
            uint64_t cumulative = 0;
            for (size_t i = 0; i <= hist.num_bounds(); i++) {
                cumulative += hist.bucket_count(i);
                prometheus::ClientMetric::Bucket bucket;
                bucket.cumulative_count = cumulative;
                bucket.upper_bound = i < hist.num_bounds()
                    ? (double)hist.bound(i) / 1e9
                    : std::numeric_limits<double>::infinity();
                metric.histogram.bucket.push_back(bucket);
            }
            // The count is what the buckets add up to, so it equals the
            // last bucket even while observations arrive.
            metric.histogram.sample_count = cumulative;
            metric.histogram.sample_sum = (double)hist.sum() / 1e9;

            family->metric.push_back(metric);
        }

        return families;
    }
};

} // namespace

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

            atomic_histograms_ = std::make_shared<AtomicHistogramCollector>();
            exposer_->RegisterCollectable(atomic_histograms_);
        }
    }

    ~Impl() {
        if (exposer_) {
            roc_log(LogInfo, "prometheus exporter: stopping");
            exposer_.reset();
        }
    }

private:
    // Declared before the exposer so that it outlives it: the exposer holds
    // only a weak reference.
    std::shared_ptr<AtomicHistogramCollector> atomic_histograms_;
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
