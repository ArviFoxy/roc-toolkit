/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "roc_audio/precise_freq_estimator.h"
#include "roc_core/log.h"
#include "roc_core/panic.h"

#include <cmath>

#ifdef ROC_TARGET_PROMETHEUS
#include "roc_metrics/prometheus.h"
#include <prometheus/family.h>
#endif

namespace roc {
namespace audio {

PreciseFreqEstimator::PreciseFreqEstimator(const PreciseFreqEstimatorConfig& config,
                                           packet::stream_timestamp_t target_latency,
                                           core::nanoseconds_t scaling_interval,
                                           const SampleSpec& sample_spec,
                                           dbgio::CsvDumper* dumper)
    : spring_gain_(config.spring_gain)
    , damping_gain_(
          // Auto-compute Butterworth-optimal K2 if not explicitly set.
          config.damping_gain > 0
              ? config.damping_gain
              : std::sqrt(2.0 * (double)sample_spec.sample_rate() * config.spring_gain))
    , integral_gain_(
          // K0 = K1 / T_I.  Set to 0 if integral_time <= 0 (PD-only).
          config.integral_time > 0
              ? config.spring_gain / config.integral_time
              : 0.0)
    , epoch_duration_((double)scaling_interval / 1e9)
    , max_correction_(config.max_correction)
    , target_latency_(target_latency)
    , xi_(0.0)
    , u_feedback_(0.0)
    , coeff_(1.0f)
    , ema_alpha_(0)
    , e_mean_ema_(0)
    , e_sq_ema_(0)
    , dumper_(dumper) {

    const double omega_n = std::sqrt((double)sample_spec.sample_rate() * spring_gain_);
    const double zeta = damping_gain_ / (2.0 * omega_n);

    // EMA time constant = 2 * tau_c, where tau_c = 1/(zeta*omega_n)
    // is the envelope decay time of the damped oscillator.
    // This averages over ~2 settling time constants for good bias estimation.
    const double tau_c = 1.0 / (zeta * omega_n);
    ema_alpha_ = epoch_duration_ / (2.0 * tau_c);
    // Guard rails for degenerate configurations only: the tau_c-derived value
    // is authoritative and must lie inside this range for any realistic gain.
    // The floor bounds the averaging horizon to ~8 min at 5 ms epochs.
    if (ema_alpha_ < 1e-5) ema_alpha_ = 1e-5;
    if (ema_alpha_ > 0.1) ema_alpha_ = 0.1;

    roc_log(LogDebug,
            "precise freq estimator: initializing:"
            " target_latency=%lu K1=%.2e K2=%.2e K0=%.2e"
            " omega_n=%.4f zeta=%.4f epoch_h=%.4f max_correction=%.4f",
            (unsigned long)target_latency_, spring_gain_, damping_gain_,
            integral_gain_, omega_n, zeta, epoch_duration_, max_correction_);

#ifdef ROC_TARGET_PROMETHEUS
    auto registry = metrics::prometheus_registry();

    freq_coeff_gauge_ =
        &prometheus::BuildGauge()
             .Name("roc_recv_freq_estimator_coeff")
             .Help("Frequency compensation coefficient (fluctuates around 1.0)")
             .Register(*registry)
             .Add({ });

    stable_gauge_ =
        &prometheus::BuildGauge()
             .Name("roc_recv_freq_estimator_stable")
             .Help("Whether the estimator has converged (0 or 1)")
             .Register(*registry)
             .Add({ });
#endif
}

float PreciseFreqEstimator::freq_coeff() const {
    return coeff_;
}

bool PreciseFreqEstimator::is_stable() const {
    // Bias-to-noise ratio test: is the transient bias small relative
    // to the steady-state noise floor?
    //
    // Var[e] = E[e²] - E[e]²  (centered variance)
    // Converged when |E[e]|² < 0.25 * Var[e], i.e. bias < 0.5 * stddev.
    //
    // During startup (e_sq_ema_ ≈ 0) or when variance estimate is
    // not yet reliable, we return false.
    const double variance = e_sq_ema_ - e_mean_ema_ * e_mean_ema_;
    if (variance <= 0) {
        return false;
    }
    return e_mean_ema_ * e_mean_ema_ < 0.25 * variance;
}

void PreciseFreqEstimator::update_target_latency(
    packet::stream_timestamp_t target) {
    target_latency_ = target;
}

void PreciseFreqEstimator::update_stream_position(
    packet::stream_timestamp_t position) {
    // Not used by this estimator. Provided for interface compatibility.
    (void)position;
}

void PreciseFreqEstimator::update(packet::stream_timestamp_t current_latency) {
    // 1. Compute queue error (in samples).
    //    Positive error = latency too high = consuming too slowly.
    const double error =
        (double)current_latency - (double)target_latency_;

    // 2. PID controller: v = K1*e + K0*xi - K2*u.
    //    K1*e   = proportional (restoring force / spring)
    //    K0*xi  = integral (eliminates steady-state bias)
    //    K2*u   = derivative / damping
    const double v = spring_gain_ * error
                   + integral_gain_ * xi_
                   - damping_gain_ * u_feedback_;

    // 3. Euler integration: u_{k+1} = u_k + v * h.
    u_feedback_ += v * epoch_duration_;

    // 4. Clamp to safety bounds.
    const bool saturated_pos = u_feedback_ > max_correction_;
    const bool saturated_neg = u_feedback_ < -max_correction_;
    double correction = u_feedback_;
    if (saturated_pos) {
        correction = max_correction_;
    }
    if (saturated_neg) {
        correction = -max_correction_;
    }

    coeff_ = (float)(1.0 + correction);

    // 5. Update integral state with anti-windup.
    //    Freeze xi when u is saturated and the error pushes in the
    //    same direction as the saturation, preventing unbounded growth.
    const bool windup = (saturated_pos && error > 0)
                     || (saturated_neg && error < 0);
    if (!windup) {
        xi_ += error * epoch_duration_;
    }

    // 5. Update queue error statistics for convergence detection.
    //    EMA alpha is derived from the system's natural time constant.
    e_mean_ema_ = (1.0 - ema_alpha_) * e_mean_ema_ + ema_alpha_ * error;
    e_sq_ema_   = (1.0 - ema_alpha_) * e_sq_ema_   + ema_alpha_ * error * error;

#ifdef ROC_TARGET_PROMETHEUS
    freq_coeff_gauge_->Set((double)coeff_);
    stable_gauge_->Set(is_stable() ? 1.0 : 0.0);
#endif

    if (dumper_) {
        // TODO: dump metrics if csv dumper is active.
    }
}

} // namespace audio
} // namespace roc
