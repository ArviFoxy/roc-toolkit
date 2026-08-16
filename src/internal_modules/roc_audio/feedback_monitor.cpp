/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_audio/feedback_monitor.h"
#include "roc_audio/packetizer.h"
#include "roc_core/log.h"
#include "roc_core/panic.h"
#include "roc_core/time.h"

#ifdef ROC_TARGET_PROMETHEUS
#include "roc_metrics/prometheus.h"
#include <prometheus/family.h>
#endif

namespace roc {
namespace audio {

FeedbackMonitor::FeedbackMonitor(IFrameWriter& writer,
                                 Packetizer& packetizer,
                                 ResamplerWriter* resampler,
                                 const FeedbackConfig& feedback_config,
                                 const LatencyConfig& latency_config,
                                 const FreqEstimatorConfig& fe_config,
                                 const SampleSpec& sample_spec,
                                 dbgio::CsvDumper* dumper)
    : tuner_(latency_config, fe_config, sample_spec, dumper)
    , use_packetizer_(false)
    , has_feedback_(false)
    , last_feedback_ts_(0)
    , feedback_timeout_(feedback_config.source_timeout)
    , packetizer_(packetizer)
    , writer_(writer)
    , resampler_(resampler)
    , enable_scaling_(latency_config.tuner_profile != LatencyTunerProfile_Intact)
    , source_(0)
    , source_change_limiter_(feedback_config.source_cooldown, 1)
    , sample_spec_(sample_spec)
    , started_(false)
    , init_status_(status::NoStatus) {
    if ((init_status_ = tuner_.init_status()) != status::StatusOK) {
        return;
    }

    if (enable_scaling_) {
        if (!init_scaling_()) {
            init_status_ = status::StatusBadConfig;
            return;
        }
    }

#ifdef ROC_TARGET_PROMETHEUS
    auto registry = metrics::prometheus_registry();

    const prometheus::Labels labels =
        metrics::scope_labels(latency_config.prometheus.scope);

    auto& e2e_fam = prometheus::BuildHistogram()
                        .Name("roc_send_e2e_latency_seconds")
                        .Help("End-to-end latency reported by receiver, in seconds")
                        .Register(*registry);
    send_e2e_latency_histogram_ = &e2e_fam.Add(
        labels, metrics::generate_histogram_buckets(latency_config.prometheus.e2e_latency));

    auto& niq_fam = prometheus::BuildHistogram()
                        .Name("roc_send_niq_latency_seconds")
                        .Help("NIQ latency reported by receiver, in seconds")
                        .Register(*registry);
    send_niq_latency_histogram_ = &niq_fam.Add(
        labels, metrics::generate_histogram_buckets(latency_config.prometheus.niq_latency));

    send_jitter_gauge_ = &prometheus::BuildGauge()
                              .Name("roc_send_jitter_mean_seconds")
                              .Help("Mean jitter reported by receiver, in seconds")
                              .Register(*registry)
                              .Add(labels);

    auto& rtt_fam = prometheus::BuildHistogram()
                        .Name("roc_send_rtt_seconds")
                        .Help("Round-trip time distribution in seconds")
                        .Register(*registry);
    send_rtt_histogram_ = &rtt_fam.Add(
        labels, metrics::generate_histogram_buckets(latency_config.prometheus.rtt));

    send_lost_packets_counter_ = &prometheus::BuildCounter()
                                      .Name("roc_send_packets_lost_total")
                                      .Help("Total packets lost as reported by receiver")
                                      .Register(*registry)
                                      .Add(labels);
#endif

    init_status_ = status::StatusOK;
}

status::StatusCode FeedbackMonitor::init_status() const {
    return init_status_;
}

bool FeedbackMonitor::is_started() const {
    return started_;
}

void FeedbackMonitor::start() {
    roc_panic_if(init_status_ != status::StatusOK);

    if (started_) {
        return;
    }

    roc_log(LogDebug, "feedback monitor: start gathering feedback");
    started_ = true;
}

void FeedbackMonitor::process_feedback(packet::stream_source_t source_id,
                                       const LatencyMetrics& latency_metrics,
                                       const packet::LinkMetrics& link_metrics) {
    roc_panic_if(init_status_ != status::StatusOK);

    if (!started_) {
        return;
    }

    if (!has_feedback_) {
        roc_log(LogInfo, "feedback monitor: got first report from receiver: source=%lu",
                (unsigned long)source_id);
        source_ = source_id;
    }

    if (has_feedback_ && source_ != source_id) {
        if (!source_change_limiter_.allow()) {
            // Protection from inadequately frequent SSRC changes.
            // Can happen is feedback monitor is mistakenly created when multiple
            // receivers exists for a single sender, which is not supported.
            // This also protects from outdated reports delivered from recently
            // restarted receiver.
            return;
        }

        roc_log(LogInfo,
                "feedback monitor: detected source change:"
                " old_source=%lu new_source=%lu",
                (unsigned long)source_, (unsigned long)source_id);

        source_ = source_id;
    }

    latency_metrics_ = latency_metrics;
    link_metrics_ = link_metrics;

    if (link_metrics_.expected_packets == 0 || use_packetizer_) {
        // If packet counter is not reported from receiver, fallback to
        // counter from sender.
        link_metrics_.expected_packets = packetizer_.metrics().encoded_packets;
        use_packetizer_ = true;
    }

    has_feedback_ = true;
    last_feedback_ts_ = core::timestamp(core::ClockMonotonic);

#ifdef ROC_TARGET_PROMETHEUS
    if (latency_metrics_.e2e_latency > 0) {
        send_e2e_latency_histogram_->Observe((double)latency_metrics_.e2e_latency / 1e9);
    }
    if (latency_metrics_.niq_latency > 0) {
        send_niq_latency_histogram_->Observe((double)latency_metrics_.niq_latency / 1e9);
    }
    if (link_metrics_.mean_jitter > 0) {
        send_jitter_gauge_->Set((double)link_metrics_.mean_jitter / 1e9);
    }
    if (link_metrics_.rtt > 0) {
        send_rtt_histogram_->Observe((double)link_metrics_.rtt / 1e9);
    }
    if (link_metrics_.lost_packets > prev_lost_packets_) {
        send_lost_packets_counter_->Increment(
            (double)(link_metrics_.lost_packets - prev_lost_packets_));
        prev_lost_packets_ = link_metrics_.lost_packets;
    }
#endif
}

status::StatusCode FeedbackMonitor::write(Frame& frame) {
    roc_panic_if(init_status_ != status::StatusOK);

    sample_spec_.validate_frame(frame);

    if (started_) {
        if (!update_tuner_(frame.duration())) {
            // TODO(gh-674): change sender SSRC to restart session
        }

        if (enable_scaling_) {
            if (!update_scaling_()) {
                return status::StatusAbort;
            }
        }
    }

    return writer_.write(frame);
}

size_t FeedbackMonitor::num_participants() const {
    // TODO(gh-674): collect per-session metrics
    return has_feedback_ ? 1 : 0;
}

const LatencyMetrics& FeedbackMonitor::latency_metrics(size_t party_index) const {
    roc_panic_if_msg(party_index >= num_participants(),
                     "feedback monitor: participant index out of bounds:"
                     " index=%lu max=%lu",
                     (unsigned long)party_index, (unsigned long)num_participants());

    // TODO(gh-674): collect per-session metrics
    return latency_metrics_;
}

const packet::LinkMetrics& FeedbackMonitor::link_metrics(size_t party_index) const {
    roc_panic_if_msg(party_index >= num_participants(),
                     "feedback monitor: participant index out of bounds:"
                     " index=%lu max=%lu",
                     (unsigned long)party_index, (unsigned long)num_participants());

    // TODO(gh-674): collect per-session metrics
    return link_metrics_;
}

bool FeedbackMonitor::update_tuner_(packet::stream_timestamp_t duration) {
    if (!has_feedback_) {
        return true;
    }

    if (core::timestamp(core::ClockMonotonic) - last_feedback_ts_ > feedback_timeout_) {
        roc_log(LogInfo,
                "feedback monitor: no reports from receiver during timeout:"
                " source=%lu timeout=%.3fms",
                (unsigned long)source_, (double)feedback_timeout_ / core::Millisecond);

        has_feedback_ = false;
        last_feedback_ts_ = 0;
        source_ = 0;

        return true;
    }

    tuner_.write_metrics(latency_metrics_, link_metrics_);

    if (!tuner_.update_stream()) {
        return false;
    }

    tuner_.advance_stream(duration);

    return true;
}

bool FeedbackMonitor::init_scaling_() {
    roc_panic_if_not(resampler_);

    if (!resampler_->set_scaling(1.0f)) {
        roc_log(LogError, "feedback monitor: can't set initial scaling");
        return false;
    }

    return true;
}

bool FeedbackMonitor::update_scaling_() {
    roc_panic_if_not(resampler_);

    const float scaling = tuner_.fetch_scaling();
    if (scaling > 0) {
        if (!resampler_->set_scaling(scaling)) {
            roc_log(LogDebug,
                    "feedback monitor: scaling factor out of bounds: scaling=%.6f",
                    (double)scaling);
            return false;
        }
    }

    return true;
}

} // namespace audio
} // namespace roc
