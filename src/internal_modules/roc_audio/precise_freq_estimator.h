/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_audio/precise_freq_estimator.h
//! @brief Precise frequency estimator using second-order feedback controller.

#ifndef ROC_AUDIO_PRECISE_FREQ_ESTIMATOR_H_
#define ROC_AUDIO_PRECISE_FREQ_ESTIMATOR_H_

#include "roc_audio/sample_spec.h"
#include "roc_core/noncopyable.h"
#include "roc_core/time.h"
#include "roc_dbgio/csv_dumper.h"
#include "roc_metrics/prometheus.h"

#include "roc_packet/units.h"

#ifdef ROC_TARGET_PROMETHEUS
#include <prometheus/gauge.h>
#endif

namespace roc {
namespace audio {

//! Precise frequency estimator config.
struct PreciseFreqEstimatorConfig {
    //! Identity of the exported metrics (name side + slot label).
    metrics::MetricsScope metrics_scope;

    //! Spring gain (K1): restoring force proportional to queue error.
    //! Controls natural frequency of the damped oscillator:
    //!   omega_n = sqrt(Fs * K1)
    //! Higher K1 = faster response, lower queue variance, more warp variation.
    double spring_gain;

    //! Damping gain (K2): damping force proportional to current warp rate.
    //! Controls damping ratio of the oscillator:
    //!   zeta = K2 / (2 * omega_n)
    //! Optimal (Butterworth) damping: K2 = sqrt(2 * Fs * K1).
    //! Set to 0 for auto-computation from spring_gain using optimal damping.
    double damping_gain;

    //! Integral settling time T_I (seconds).
    //! Controls the integral gain: K0 = K1 / T_I.
    //! The integral mode eliminates steady-state bias with this time constant.
    //! Set to 0 to disable integral action (PD-only mode).
    double integral_time;

    //! Maximum freq_coeff deviation from 1.0.
    double max_correction;

    PreciseFreqEstimatorConfig()
        : spring_gain(1e-5)
        , damping_gain(0)
        , integral_time(30.0)
        , max_correction(0.005) {
    }
};

//! Precise frequency estimator.
//!
//! @b Overview
//!
//! Implements a PID controller (Type 2 system) for clock drift
//! compensation and latency regulation. The controller combines:
//!  - Integral action (K0): eliminates steady-state bias for any drift
//!  - Proportional action (K1): restoring force on queue error
//!  - Derivative/damping action (K2): damps warp oscillations
//!
//! @b Plant model (fluid approximation)
//!
//! Under the fluid approximation, the queue error e = q - q_target evolves as:
//!
//!   de = Fs * (delta - u) * dtau + sigma_w * dW
//!
//! where delta is the clock drift, u is the warp rate (freq_coeff - 1), and
//! W is a Wiener process representing the accumulated jitter noise.
//!
//! @b PID control law
//!
//! The controller maintains an integral state xi and warp state u:
//!
//!   dxi/dtau = e                          (integral of error)
//!   du/dtau  = K1*e + K0*xi - K2*u        (PID control)
//!
//! The full output is: freq_coeff = 1 + u
//!
//! @b Closed-loop dynamics
//!
//! The closed-loop characteristic polynomial is cubic:
//!
//!   s^3 + K2*s^2 + Fs*K1*s + Fs*K0 = 0
//!
//! With dominant-pole design, this factors approximately as:
//!   (s^2 + 2*zeta*omega_n*s + omega_n^2) * (s + p0)
//! where omega_n = sqrt(Fs*K1), zeta = K2/(2*omega_n), p0 = 1/T_I.
//!
//! @b H2-optimal gains with zero bias
//!
//! The PD gains are Butterworth-optimal (zeta = 1/sqrt(2)):
//!   K1 = 1/sqrt(lambda)     (aggressiveness parameter)
//!   K2 = sqrt(2 * Fs * K1)  (auto-computed)
//! The integral gain is set by its settling time T_I:
//!   K0 = K1 / T_I
//!
//! This achieves:
//!  - Zero steady-state bias (E[e] = 0) for any constant drift
//!  - Variance/smoothness tradeoff controlled by K1
//!  - Pareto improvement over PD: same variance, zero bias
//!
//! See docs/latex/controller.tex for the full mathematical derivation.
class PreciseFreqEstimator : public core::NonCopyable<> {
public:
    //! Initialize.
    //! @p scaling_interval is the control epoch duration (time between
    //! consecutive update() calls), used for Euler integration of the
    //! warp state.
    PreciseFreqEstimator(const PreciseFreqEstimatorConfig& config,
                         packet::stream_timestamp_t target_latency,
                         core::nanoseconds_t scaling_interval,
                         const SampleSpec& sample_spec,
                         dbgio::CsvDumper* dumper);

    //! Get current frequency coefficient.
    //! Output is close to 1.0: value > 1.0 means consume faster (receiver is slow),
    //! value < 1.0 means consume slower (receiver is fast).
    float freq_coeff() const;

    //! Check if the estimator has converged to steady state.
    //! True when the queue error bias (transient) is small compared
    //! to the noise-driven fluctuations (stationary variance).
    //! Uses a bias-to-noise ratio: |E[e]|² < 0.25 * Var[e].
    bool is_stable() const;

    //! Update target latency.
    void update_target_latency(packet::stream_timestamp_t target);

    //! Update stream position (for compatibility, not used internally).
    void update_stream_position(packet::stream_timestamp_t position);

    //! Update current latency.
    //! Called once per scaling interval.
    void update(packet::stream_timestamp_t current_latency);

private:
    // PID controller gains.
    const double spring_gain_;    // K1: proportional (restoring force).
    const double damping_gain_;   // K2: derivative (damping on warp rate).
    const double integral_gain_;  // K0: integral (eliminates bias). K0 = K1/T_I.

    const double epoch_duration_; // h: control epoch in seconds.
    const double max_correction_;

    packet::stream_timestamp_t target_latency_;

    // Controller state (persists between update calls).
    double xi_;         // Integral of queue error (integral state).
    double u_feedback_; // Warp rate (integrator output).
    float coeff_;

    // Convergence detection: EMA of queue error statistics.
    // The EMA alpha is derived from the system's natural time constant
    // tau_c = sqrt(2) / omega_n, averaging over 2*tau_c.
    double ema_alpha_;   // EMA discount factor.
    double e_mean_ema_;  // EMA of e (tracks bias / mean error).
    double e_sq_ema_;    // EMA of e² (tracks second moment).

    dbgio::CsvDumper* dumper_;

#ifdef ROC_TARGET_PROMETHEUS
    prometheus::Gauge* freq_coeff_gauge_;
    prometheus::Gauge* stable_gauge_;
#endif
};

} // namespace audio
} // namespace roc

#endif // ROC_AUDIO_PRECISE_FREQ_ESTIMATOR_H_
