/*
 * Copyright (c) 2024 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include <CppUTest/TestHarness.h>

#include "roc_audio/freq_estimator.h"
#include "roc_audio/latency_tuner.h"
#include "roc_core/macro_helpers.h"
#include "roc_core/optional.h"
#include "roc_core/time.h"
#include "roc_packet/units.h"
#include "roc_status/status_code.h"

#include <math.h>

namespace roc {
namespace audio {

namespace {

enum {
    SampleRate = 44100,
    NumCh = 1,
    ChMask = 0x1
};

const SampleSpec sample_spec(
    SampleRate, PcmSubformat_Raw, ChanLayout_Surround, ChanOrder_Smpte, ChMask);

// Duration of one "step" in stream timestamps.
// Matches the default scaling_interval of 5ms.
const packet::stream_timestamp_t StepDuration =
    (packet::stream_timestamp_t)(SampleRate * 5 / 1000); // 220.5 -> 220

const core::nanoseconds_t Ms = core::Millisecond;

const double ScalingEpsilon = 0.0001;

// Bundles LatencyTuner with the FreqEstimatorConfig it requires, deriving
// the estimator defaults from the latency profile the way production callers
// do (latency_monitor.cpp, feedback_monitor.cpp). Exposes the subset of the
// tuner API the tests use, so test bodies read as if holding the tuner.
struct TestTuner {
    FreqEstimatorConfig fe_config;
    core::Optional<LatencyTuner> tuner;

    explicit TestTuner(const LatencyConfig& config) {
        CHECK(fe_config.deduce_defaults(config.tuner_profile));
        tuner.reset(new (tuner) LatencyTuner(config, fe_config, sample_spec, NULL));
    }

    bool is_valid() {
        return tuner->init_status() == status::StatusOK;
    }

    operator LatencyTuner&() {
        return *tuner;
    }

    void write_metrics(const LatencyMetrics& lm, const packet::LinkMetrics& link) {
        tuner->write_metrics(lm, link);
    }

    bool update_stream() {
        return tuner->update_stream();
    }

    void advance_stream(packet::stream_timestamp_t duration) {
        tuner->advance_stream(duration);
    }

    float fetch_scaling() {
        return tuner->fetch_scaling();
    }
};

// -- Config helpers --

LatencyConfig make_config(LatencyTunerBackend backend,
                          LatencyTunerProfile profile,
                          core::nanoseconds_t target_latency,
                          core::nanoseconds_t latency_tolerance) {
    LatencyConfig config;
    config.tuner_backend = backend;
    config.tuner_profile = profile;
    config.target_latency = target_latency;
    if (latency_tolerance > 0) {
        config.latency_tolerance = latency_tolerance;
    }
    config.deduce_defaults(target_latency, true /* is_receiver */);
    return config;
}

LatencyConfig make_niq_config(LatencyTunerProfile profile,
                              core::nanoseconds_t target_latency,
                              core::nanoseconds_t latency_tolerance = 0) {
    return make_config(LatencyTunerBackend_Niq, profile, target_latency, latency_tolerance);
}

LatencyConfig make_e2e_config(LatencyTunerProfile profile,
                              core::nanoseconds_t target_latency,
                              core::nanoseconds_t latency_tolerance = 0) {
    return make_config(LatencyTunerBackend_E2e, profile, target_latency, latency_tolerance);
}

// -- Step helpers --

// Feed NIQ latency metric and advance one step.
bool step_niq(LatencyTuner& tuner,
              core::nanoseconds_t niq_latency,
              core::nanoseconds_t niq_stalling = 0) {
    LatencyMetrics lm;
    lm.niq_latency = niq_latency;
    lm.niq_stalling = niq_stalling;
    packet::LinkMetrics link;
    tuner.write_metrics(lm, link);
    bool ok = tuner.update_stream();
    tuner.advance_stream(StepDuration);
    return ok;
}

// Feed E2E latency metric and advance one step.
bool step_e2e(LatencyTuner& tuner,
              core::nanoseconds_t e2e_latency) {
    LatencyMetrics lm;
    lm.e2e_latency = e2e_latency;
    packet::LinkMetrics link;
    tuner.write_metrics(lm, link);
    bool ok = tuner.update_stream();
    tuner.advance_stream(StepDuration);
    return ok;
}

// Feed both NIQ and E2E metrics and advance one step.
bool step_both(LatencyTuner& tuner,
               core::nanoseconds_t niq_latency,
               core::nanoseconds_t e2e_latency,
               core::nanoseconds_t niq_stalling = 0) {
    LatencyMetrics lm;
    lm.niq_latency = niq_latency;
    lm.e2e_latency = e2e_latency;
    lm.niq_stalling = niq_stalling;
    packet::LinkMetrics link;
    tuner.write_metrics(lm, link);
    bool ok = tuner.update_stream();
    tuner.advance_stream(StepDuration);
    return ok;
}

// Run N steps feeding constant latency, return final scaling.
float run_steps_niq(LatencyTuner& tuner,
                    core::nanoseconds_t niq_latency,
                    size_t n_steps) {
    float scaling = 0;
    for (size_t i = 0; i < n_steps; i++) {
        step_niq(tuner, niq_latency);
        float s = tuner.fetch_scaling();
        if (s != 0) {
            scaling = s;
        }
    }
    return scaling;
}

float run_steps_e2e(LatencyTuner& tuner,
                    core::nanoseconds_t e2e_latency,
                    size_t n_steps) {
    float scaling = 0;
    for (size_t i = 0; i < n_steps; i++) {
        step_e2e(tuner, e2e_latency);
        float s = tuner.fetch_scaling();
        if (s != 0) {
            scaling = s;
        }
    }
    return scaling;
}

// -- Simulation infrastructure --

// Simple deterministic LCG PRNG for reproducible tests.
struct Rng {
    unsigned state;

    Rng(unsigned seed)
        : state(seed) {
    }

    // Returns uniform random in [0, 1).
    double uniform() {
        state = state * 1103515245u + 12345u;
        return (double)((state >> 16) & 0x7FFF) / 32768.0;
    }

    // Returns approximate Gaussian via Box-Muller.
    double gaussian(double mean, double stddev) {
        double u1 = uniform();
        double u2 = uniform();
        // Avoid log(0).
        if (u1 < 1e-10) {
            u1 = 1e-10;
        }
        double z = sqrt(-2.0 * log(u1)) * cos(2.0 * 3.14159265358979 * u2);
        return mean + stddev * z;
    }
};

struct SimConfig {
    double drift_ppm;
    double jitter_stddev_ms;
    double burst_spike_ms;
    double burst_interval_ms;
    double burst_duration_ms;
    unsigned seed;

    SimConfig()
        : drift_ppm(0)
        , jitter_stddev_ms(0)
        , burst_spike_ms(0)
        , burst_interval_ms(0)
        , burst_duration_ms(0)
        , seed(42) {
    }
};

struct SimResult {
    double final_latency_ms;
    double max_scaling_deviation;
    double scaling_variance;
    bool bounds_violated;
    size_t num_steps;

    SimResult()
        : final_latency_ms(0)
        , max_scaling_deviation(0)
        , scaling_variance(0)
        , bounds_violated(false)
        , num_steps(0) {
    }
};

SimResult run_simulation(LatencyTunerBackend backend,
                         LatencyTunerProfile profile,
                         core::nanoseconds_t target_latency,
                         core::nanoseconds_t tolerance,
                         const SimConfig& sim,
                         size_t num_steps) {
    LatencyConfig config = make_config(backend, profile, target_latency, tolerance);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    Rng rng(sim.seed);
    SimResult result;
    result.num_steps = num_steps;

    const double step_ms = 5.0; // 5ms per step
    double latency_ms = (double)target_latency / (double)Ms;
    double drift_per_step_ms = sim.drift_ppm * step_ms / 1e6;

    // For computing scaling variance over last quarter of steps.
    const size_t variance_window = num_steps / 4;
    double* scaling_history = new double[variance_window];
    size_t scaling_idx = 0;
    double scaling_sum = 0;
    double scaling_sum_sq = 0;
    size_t scaling_count = 0;

    for (size_t i = 0; i < num_steps; i++) {
        // Apply clock drift.
        latency_ms += drift_per_step_ms;

        // Apply jitter.
        double jitter = 0;
        if (sim.jitter_stddev_ms > 0) {
            jitter = rng.gaussian(0, sim.jitter_stddev_ms);
        }

        // Apply burst spike.
        double burst = 0;
        if (sim.burst_interval_ms > 0 && sim.burst_duration_ms > 0) {
            double time_ms = (double)i * step_ms;
            double cycle_pos = fmod(time_ms, sim.burst_interval_ms);
            if (cycle_pos < sim.burst_duration_ms) {
                burst = sim.burst_spike_ms;
            }
        }

        double observed_latency_ms = latency_ms + jitter + burst;
        if (observed_latency_ms < 0) {
            observed_latency_ms = 0;
        }
        core::nanoseconds_t observed_ns =
            (core::nanoseconds_t)(observed_latency_ms * (double)Ms);

        // Feed to tuner.
        bool ok;
        if (backend == LatencyTunerBackend_Niq) {
            ok = step_niq(tuner, observed_ns);
        } else {
            ok = step_e2e(tuner, observed_ns);
        }

        if (!ok) {
            result.bounds_violated = true;
        }

        // Get scaling and apply feedback.
        float scaling = tuner.fetch_scaling();
        if (scaling > 0) {
            double deviation = fabs((double)scaling - 1.0);
            if (deviation > result.max_scaling_deviation) {
                result.max_scaling_deviation = deviation;
            }

            // Model resampler feedback: scaling > 1.0 means play faster,
            // which drains the queue, reducing latency.
            latency_ms -= ((double)scaling - 1.0) * step_ms;

            // Track scaling statistics for variance computation.
            if (i >= num_steps - variance_window) {
                if (scaling_count < variance_window) {
                    scaling_history[scaling_count] = (double)scaling;
                }
                scaling_sum += (double)scaling;
                scaling_sum_sq += (double)scaling * (double)scaling;
                scaling_count++;
            }
        }
    }

    result.final_latency_ms = latency_ms;

    if (scaling_count > 1) {
        double mean = scaling_sum / (double)scaling_count;
        result.scaling_variance =
            (scaling_sum_sq / (double)scaling_count) - (mean * mean);
    }

    delete[] scaling_history;
    return result;
}

} // namespace

// =============================================================================
// Group 1: Initialization & Defaults
// =============================================================================

TEST_GROUP(latency_tuner_init) {};

TEST(latency_tuner_init, niq_defaults_gradual_high_latency) {
    // NIQ with target >= 30ms should default to Gradual profile.
    LatencyConfig config;
    config.tuner_backend = LatencyTunerBackend_Niq;
    config.target_latency = 200 * Ms;
    config.deduce_defaults(200 * Ms, true);

    CHECK_EQUAL(LatencyTunerProfile_Gradual, config.tuner_profile);

    TestTuner tuner(config);
    CHECK(tuner.is_valid());
}

TEST(latency_tuner_init, niq_defaults_responsive_low_latency) {
    // NIQ with target < 30ms should default to Responsive profile.
    LatencyConfig config;
    config.tuner_backend = LatencyTunerBackend_Niq;
    config.target_latency = 20 * Ms;
    config.deduce_defaults(20 * Ms, true);

    CHECK_EQUAL(LatencyTunerProfile_Responsive, config.tuner_profile);

    TestTuner tuner(config);
    CHECK(tuner.is_valid());
}

TEST(latency_tuner_init, e2e_defaults_responsive_always) {
    // E2E should always default to Responsive, even at high latency.
    LatencyConfig config;
    config.tuner_backend = LatencyTunerBackend_E2e;
    config.target_latency = 200 * Ms;
    config.deduce_defaults(200 * Ms, true);

    CHECK_EQUAL(LatencyTunerProfile_Responsive, config.tuner_profile);

    TestTuner tuner(config);
    CHECK(tuner.is_valid());
}

TEST(latency_tuner_init, intact_profile_no_scaling) {
    // Intact profile should disable tuning entirely.
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Intact, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed metrics and advance.
    run_steps_niq(tuner, 300 * Ms, 100);

    // fetch_scaling should always return 0 (no scaling computed).
    float s = tuner.fetch_scaling();
    DOUBLES_EQUAL(0.0, (double)s, 0.0);
}

TEST(latency_tuner_init, observation_accessors) {
    // last_freq_coeff() / last_target_latency() are non-consuming
    // observation accessors: reading them must not affect what the next
    // fetch_scaling() returns.
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 40 * Ms, 40 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Before any scaling was computed.
    DOUBLES_EQUAL(0.0, (double)tuner.tuner->last_freq_coeff(), 0.0);
    LONGLONGS_EQUAL(40 * Ms, tuner.tuner->last_target_latency());

    // Feed steady off-target latency until a scaling is computed, but
    // observe BEFORE fetching.
    float observed = 0;
    float fetched = 0;
    for (size_t i = 0; i < 1000; i++) {
        step_niq(tuner, 50 * Ms);
        observed = tuner.tuner->last_freq_coeff();
        fetched = tuner.fetch_scaling();
        if (fetched != 0) {
            break;
        }
    }

    // A scaling arrived, and the observation accessor saw the same value
    // without consuming it.
    CHECK(fetched != 0);
    DOUBLES_EQUAL((double)fetched, (double)observed, 1e-9);

    // Repeated observation returns the same value.
    DOUBLES_EQUAL((double)observed, (double)tuner.tuner->last_freq_coeff(), 0.0);

    // Target latency stays reported in ns.
    LONGLONGS_EQUAL(40 * Ms, tuner.tuner->last_target_latency());
}

TEST(latency_tuner_init, valid_configs) {
    // Every backend × profile combination should initialize successfully.
    const LatencyTunerBackend backends[] = {
        LatencyTunerBackend_Niq, LatencyTunerBackend_E2e
    };
    const LatencyTunerProfile profiles[] = {
        LatencyTunerProfile_Responsive, LatencyTunerProfile_Gradual
    };
    for (size_t b = 0; b < ROC_ARRAY_SIZE(backends); b++) {
        for (size_t p = 0; p < ROC_ARRAY_SIZE(profiles); p++) {
            LatencyConfig config =
                make_config(backends[b], profiles[p], 200 * Ms, 200 * Ms);
            TestTuner tuner(config);
            CHECK(tuner.is_valid());
        }
    }
}

// =============================================================================
// Group 2: Bounds Checking
// =============================================================================

TEST_GROUP(latency_tuner_bounds) {};

TEST(latency_tuner_bounds, niq_within_bounds) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Latency at target — should be within bounds.
    CHECK(step_niq(tuner, 200 * Ms));

    // Latency near upper bound but within.
    CHECK(step_niq(tuner, 290 * Ms));

    // Latency near lower bound but within.
    CHECK(step_niq(tuner, 110 * Ms));
}

TEST(latency_tuner_bounds, niq_exceeds_upper) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Latency above upper bound.
    CHECK(!step_niq(tuner, 310 * Ms));
}

TEST(latency_tuner_bounds, niq_exceeds_lower) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Latency below lower bound, not stalling.
    CHECK(!step_niq(tuner, 90 * Ms, 0));
}

TEST(latency_tuner_bounds, niq_stalling_suppresses_lower_bound) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Latency below lower bound, but niq_stalling is high.
    // stale_tolerance is deduced as latency_tolerance / 4 = 25ms.
    // When stalling > stale_tolerance AND latency < min, bounds check passes
    // (doesn't kill session during burst loss).
    CHECK(step_niq(tuner, 90 * Ms, 30 * Ms));
}

TEST(latency_tuner_bounds, e2e_within_bounds) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    CHECK(step_e2e(tuner, 200 * Ms));
    CHECK(step_e2e(tuner, 290 * Ms));
    CHECK(step_e2e(tuner, 110 * Ms));
}

TEST(latency_tuner_bounds, e2e_exceeds_upper) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    CHECK(!step_e2e(tuner, 310 * Ms));
}

TEST(latency_tuner_bounds, e2e_exceeds_lower) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    CHECK(!step_e2e(tuner, 90 * Ms));
}

TEST(latency_tuner_bounds, e2e_no_stalling_exception) {
    // E2E should NOT have the stalling exception.
    // Even with high niq_stalling, E2E below lower bound should still fail.
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 100 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed both E2E and NIQ metrics. E2E is below bounds, NIQ stalling is high.
    // Backend is E2E, so stalling exception should NOT apply.
    LatencyMetrics lm;
    lm.e2e_latency = 90 * Ms;
    lm.niq_latency = 90 * Ms;
    lm.niq_stalling = 30 * Ms;
    packet::LinkMetrics link;
    tuner.write_metrics(lm, link);
    CHECK(!tuner.update_stream());
}

// =============================================================================
// Group 3: Scaling / Convergence
// =============================================================================

TEST_GROUP(latency_tuner_scaling) {};

TEST(latency_tuner_scaling, niq_no_metrics_no_scaling) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Without writing any metrics, update should succeed but no scaling.
    CHECK(tuner.update_stream());
    tuner.advance_stream(StepDuration);
    DOUBLES_EQUAL(0.0, (double)tuner.fetch_scaling(), 0.0);
}

TEST(latency_tuner_scaling, niq_at_target) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    float scaling = run_steps_niq(tuner, 200 * Ms, 500);

    // At target, scaling should be very close to 1.0.
    DOUBLES_EQUAL(1.0, (double)scaling, ScalingEpsilon);
}

TEST(latency_tuner_scaling, niq_above_target) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed latency significantly above target.
    float scaling = run_steps_niq(tuner, 300 * Ms, 500);

    // Scaling should be > 1.0 (speed up playback to drain queue).
    CHECK(scaling > 1.0);
}

TEST(latency_tuner_scaling, niq_below_target) {
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed latency below target.
    float scaling = run_steps_niq(tuner, 150 * Ms, 500);

    // Scaling should be < 1.0 (slow down playback to let queue grow).
    CHECK(scaling < 1.0);
}

TEST(latency_tuner_scaling, e2e_no_metrics_no_scaling) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Without E2E metrics, tuner should not produce scaling.
    CHECK(tuner.update_stream());
    tuner.advance_stream(StepDuration);
    DOUBLES_EQUAL(0.0, (double)tuner.fetch_scaling(), 0.0);
}

TEST(latency_tuner_scaling, e2e_at_target) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    float scaling = run_steps_e2e(tuner, 200 * Ms, 500);

    DOUBLES_EQUAL(1.0, (double)scaling, ScalingEpsilon);
}

TEST(latency_tuner_scaling, e2e_above_target) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    float scaling = run_steps_e2e(tuner, 300 * Ms, 500);

    CHECK(scaling > 1.0);
}

TEST(latency_tuner_scaling, e2e_below_target) {
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    float scaling = run_steps_e2e(tuner, 150 * Ms, 500);

    CHECK(scaling < 1.0);
}

TEST(latency_tuner_scaling, e2e_no_epoch_replay_after_idle_start) {
    // The e2e backend is idle until the first RTCP mapping arrives, while
    // the stream position keeps advancing. The epoch grid is anchored at
    // the first computed sample, so a tuner with a long metric-less
    // preamble produces exactly the same scaling as a fresh tuner fed the
    // same single e2e sample: no backlogged epochs are replayed.
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);

    TestTuner idle_tuner(config);
    TestTuner fresh_tuner(config);
    CHECK(idle_tuner.is_valid());
    CHECK(fresh_tuner.is_valid());

    // Long preamble without metrics: stream advances, no scaling computed.
    for (size_t i = 0; i < 2000; i++) {
        CHECK(idle_tuner.update_stream());
        idle_tuner.advance_stream(StepDuration);
    }
    DOUBLES_EQUAL(0.0, (double)idle_tuner.fetch_scaling(), 0.0);

    // One identical e2e sample into both tuners.
    step_e2e(idle_tuner, 230 * Ms);
    step_e2e(fresh_tuner, 230 * Ms);

    const float idle_scaling = idle_tuner.fetch_scaling();
    const float fresh_scaling = fresh_tuner.fetch_scaling();

    CHECK(fresh_scaling != 0);
    DOUBLES_EQUAL((double)fresh_scaling, (double)idle_scaling, 0.0);
}

TEST(latency_tuner_scaling, e2e_ignores_niq) {
    // When backend=E2E, NIQ metrics should have no effect on scaling.
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed only NIQ metrics (no E2E). Tuner should not produce scaling.
    for (size_t i = 0; i < 500; i++) {
        LatencyMetrics lm;
        lm.niq_latency = 300 * Ms; // Way above target.
        lm.e2e_latency = 0;        // No E2E metric.
        packet::LinkMetrics link;
        tuner.write_metrics(lm, link);
        tuner.update_stream();
        tuner.advance_stream(StepDuration);
    }

    // Should not have produced any scaling since no E2E metrics were provided.
    DOUBLES_EQUAL(0.0, (double)tuner.fetch_scaling(), 0.0);
}

TEST(latency_tuner_scaling, niq_ignores_e2e) {
    // When backend=NIQ, E2E metrics should have no effect on scaling.
    LatencyConfig config =
        make_niq_config(LatencyTunerProfile_Responsive, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed NIQ at target, E2E way above target.
    for (size_t i = 0; i < 500; i++) {
        step_both(tuner, 200 * Ms, 500 * Ms);
    }

    float scaling = tuner.fetch_scaling();
    // NIQ is at target, so scaling should be ~1.0 despite E2E being way off.
    if (scaling != 0) {
        DOUBLES_EQUAL(1.0, (double)scaling, ScalingEpsilon);
    }
}

// =============================================================================
// Group 4: E2E-Specific Behavior
// =============================================================================

TEST_GROUP(latency_tuner_e2e) {};

TEST(latency_tuner_e2e, e2e_uses_responsive_by_default) {
    // Even at high latency, E2E should default to Responsive.
    LatencyConfig config;
    config.tuner_backend = LatencyTunerBackend_E2e;
    config.target_latency = 500 * Ms;
    config.deduce_defaults(500 * Ms, true);

    CHECK_EQUAL(LatencyTunerProfile_Responsive, config.tuner_profile);
}

TEST(latency_tuner_e2e, e2e_convergence_direction_gradual) {
    // E2E + Gradual: verify convergence direction is correct.
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Gradual, 200 * Ms, 200 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // Feed above target for many steps (Gradual is slow).
    float scaling = run_steps_e2e(tuner, 300 * Ms, 5000);

    // Should still converge upward (scaling > 1.0).
    CHECK(scaling > 1.0);
}

TEST(latency_tuner_e2e, e2e_bounds_no_stalling) {
    // E2E bounds checking should never consider stalling.
    // This is the same as e2e_no_stalling_exception but with different framing.
    LatencyConfig config =
        make_e2e_config(LatencyTunerProfile_Responsive, 200 * Ms, 50 * Ms);
    TestTuner tuner(config);
    CHECK(tuner.is_valid());

    // E2E latency below lower bound (200 - 50 = 150ms).
    // Even with stalling, should still trigger out-of-bounds.
    LatencyMetrics lm;
    lm.e2e_latency = 140 * Ms;
    lm.niq_stalling = 100 * Ms; // Very high stalling.
    packet::LinkMetrics link;
    tuner.write_metrics(lm, link);
    CHECK(!tuner.update_stream());
}

// =============================================================================
// Group 5: Simulation Tests
// =============================================================================

TEST_GROUP(latency_tuner_sim) {};

TEST(latency_tuner_sim, niq_clock_drift_positive) {
    // Sender clock 50ppm faster — NIQ latency gradually rises.
    // Tuner should compensate and stabilize near target.
    SimConfig sim;
    sim.drift_ppm = 50.0;
    sim.seed = 1;

    SimResult result = run_simulation(LatencyTunerBackend_Niq,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    // Final latency should be within 10% of 200ms target.
    CHECK(result.final_latency_ms > 180.0);
    CHECK(result.final_latency_ms < 220.0);
}

TEST(latency_tuner_sim, niq_clock_drift_negative) {
    // Sender clock 50ppm slower — NIQ latency gradually falls.
    SimConfig sim;
    sim.drift_ppm = -50.0;
    sim.seed = 2;

    SimResult result = run_simulation(LatencyTunerBackend_Niq,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    CHECK(result.final_latency_ms > 180.0);
    CHECK(result.final_latency_ms < 220.0);
}

TEST(latency_tuner_sim, e2e_clock_drift_positive) {
    // Same drift model fed through E2E metric.
    SimConfig sim;
    sim.drift_ppm = 50.0;
    sim.seed = 3;

    SimResult result = run_simulation(LatencyTunerBackend_E2e,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    CHECK(result.final_latency_ms > 180.0);
    CHECK(result.final_latency_ms < 220.0);
}

TEST(latency_tuner_sim, niq_jitter_stability) {
    // No drift, just jitter. Tuner should stay stable.
    SimConfig sim;
    sim.jitter_stddev_ms = 10.0; // 5% of 200ms target.
    sim.seed = 4;

    SimResult result = run_simulation(LatencyTunerBackend_Niq,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    // Scaling should stay very close to 1.0.
    CHECK(result.max_scaling_deviation < 0.005);
    // Final latency should be near target (no drift to cause deviation).
    CHECK(result.final_latency_ms > 170.0);
    CHECK(result.final_latency_ms < 230.0);
}

TEST(latency_tuner_sim, e2e_jitter_stability) {
    SimConfig sim;
    sim.jitter_stddev_ms = 10.0;
    sim.seed = 5;

    SimResult result = run_simulation(LatencyTunerBackend_E2e,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    CHECK(result.max_scaling_deviation < 0.005);
    CHECK(result.final_latency_ms > 170.0);
    CHECK(result.final_latency_ms < 230.0);
}

TEST(latency_tuner_sim, niq_burst_delay_recovery) {
    // WiFi-like burst: every 2s, latency spikes +100ms for 200ms.
    SimConfig sim;
    sim.burst_spike_ms = 100.0;
    sim.burst_interval_ms = 2000.0;
    sim.burst_duration_ms = 200.0;
    sim.seed = 6;

    SimResult result = run_simulation(LatencyTunerBackend_Niq,
                                      LatencyTunerProfile_Gradual,
                                      200 * Ms, 200 * Ms, sim, 30000);

    // With wide tolerance (200ms), bursts of +100ms should not violate bounds.
    CHECK(!result.bounds_violated);
}

TEST(latency_tuner_sim, niq_drift_plus_jitter) {
    // The realistic scenario: 50ppm drift + Gaussian jitter.
    SimConfig sim;
    sim.drift_ppm = 50.0;
    sim.jitter_stddev_ms = 10.0;
    sim.seed = 7;

    SimResult result = run_simulation(LatencyTunerBackend_Niq,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 30000);

    CHECK(!result.bounds_violated);
    // Should still converge despite noise.
    CHECK(result.final_latency_ms > 170.0);
    CHECK(result.final_latency_ms < 230.0);
}

TEST(latency_tuner_sim, e2e_drift_plus_jitter) {
    SimConfig sim;
    sim.drift_ppm = 50.0;
    sim.jitter_stddev_ms = 10.0;
    sim.seed = 8;

    SimResult result = run_simulation(LatencyTunerBackend_E2e,
                                      LatencyTunerProfile_Responsive,
                                      200 * Ms, 200 * Ms, sim, 30000);

    CHECK(!result.bounds_violated);
    CHECK(result.final_latency_ms > 170.0);
    CHECK(result.final_latency_ms < 230.0);
}

TEST(latency_tuner_sim, gradual_vs_responsive_jitter) {
    // Gradual should produce lower scaling variance than Responsive under jitter.
    SimConfig sim;
    sim.jitter_stddev_ms = 15.0;
    sim.seed = 9;

    SimResult result_responsive = run_simulation(LatencyTunerBackend_Niq,
                                                 LatencyTunerProfile_Responsive,
                                                 200 * Ms, 200 * Ms, sim, 30000);

    SimResult result_gradual = run_simulation(LatencyTunerBackend_Niq,
                                              LatencyTunerProfile_Gradual,
                                              200 * Ms, 200 * Ms, sim, 30000);

    CHECK(!result_responsive.bounds_violated);
    CHECK(!result_gradual.bounds_violated);

    // Gradual applies bounded, decimated adjustments, so its PEAK scaling
    // deviation stays below Responsive's under identical jitter. Sampled
    // scaling variance is not comparable between the profiles: for Gradual,
    // fetch_scaling() returns sparse decimated updates, which inflates the
    // sample-to-sample spread even though each adjustment is smaller
    // (measured: gradual maxdev ~1e-4 vs responsive ~4e-4, while gradual
    // sampled variance is ~100x larger).
    CHECK(result_gradual.max_scaling_deviation
          < result_responsive.max_scaling_deviation);
}

TEST(latency_tuner_sim, e2e_multi_receiver_convergence) {
    // Two E2E tuners with different initial latency offsets should converge
    // to the same scaling factor. This validates multi-speaker sync property.
    const core::nanoseconds_t target = 200 * Ms;
    const core::nanoseconds_t tolerance = 200 * Ms;

    LatencyConfig config1 =
        make_e2e_config(LatencyTunerProfile_Responsive, target, tolerance);
    LatencyConfig config2 =
        make_e2e_config(LatencyTunerProfile_Responsive, target, tolerance);
    TestTuner tuner1(config1);
    TestTuner tuner2(config2);
    CHECK(tuner1.is_valid());
    CHECK(tuner2.is_valid());

    // Simulate two receivers with different initial E2E offsets.
    double latency1_ms = 220.0; // +20ms offset.
    double latency2_ms = 180.0; // -20ms offset.
    const double step_ms = 5.0;
    const double drift_ppm = 30.0;
    const double drift_per_step = drift_ppm * step_ms / 1e6;

    float scaling1 = 1.0f;
    float scaling2 = 1.0f;

    for (size_t i = 0; i < 30000; i++) {
        // Both receivers see the same clock drift.
        latency1_ms += drift_per_step;
        latency2_ms += drift_per_step;

        core::nanoseconds_t lat1_ns = (core::nanoseconds_t)(latency1_ms * (double)Ms);
        core::nanoseconds_t lat2_ns = (core::nanoseconds_t)(latency2_ms * (double)Ms);

        step_e2e(tuner1, lat1_ns);
        step_e2e(tuner2, lat2_ns);

        float s1 = tuner1.fetch_scaling();
        float s2 = tuner2.fetch_scaling();

        if (s1 > 0) {
            scaling1 = s1;
            latency1_ms -= ((double)s1 - 1.0) * step_ms;
        }
        if (s2 > 0) {
            scaling2 = s2;
            latency2_ms -= ((double)s2 - 1.0) * step_ms;
        }
    }

    // After convergence, both latencies should be near target.
    CHECK(fabs(latency1_ms - 200.0) < 20.0);
    CHECK(fabs(latency2_ms - 200.0) < 20.0);

    // And both scaling factors should be very close to each other.
    // (They should converge to compensating the same drift.)
    DOUBLES_EQUAL((double)scaling1, (double)scaling2, 0.001);
}

// =============================================================================
// Group 6: Second-order controller simulations
// =============================================================================
//
// The second-order profile runs PreciseFreqEstimator: a PID (Type 2)
// controller with spring gain K1 (default 1e-5), Butterworth damping
// zeta = 1/sqrt(2), and integral time T_I = 30 s. Closed forms used for the
// bands below (see docs/latex/controller.tex):
//
//   omega_n = sqrt(Fs * K1) = sqrt(44100 * 1e-5) ~= 0.66 rad/s
//   settle (2%) ~= 4 / (zeta * omega_n) ~= 8.5 s  (1700 steps)
//   PD drift bias  e_ss = (K2/K1) * drift  ~= 0.1 ms per 50 ppm,
//     then eliminated by the integral with time constant T_I
//   white measurement noise sigma_n per 5 ms epoch injects true-latency
//     variance  S_n * omega_n / (2*sqrt(2)),  S_n = sigma_n^2 * h:
//     for sigma_n = 10 ms this is ~0.34 ms stddev
//   sensor-noise warp wander: Var[u] ~= (K1*sigma_n)^2 * h / (2*K2)
//     -> sigma_u ~= 2.3e-4 for sigma_n = 10 ms

TEST_GROUP(latency_tuner_second_order) {};

TEST(latency_tuner_second_order, e2e_drift) {
    // Pure 50 ppm drift: the integral drives the bias to zero, and the warp
    // settles at the drift value without approaching the 0.5% clamp.
    SimConfig sim;
    sim.drift_ppm = 50.0;
    sim.seed = 10;

    SimResult result = run_simulation(LatencyTunerBackend_E2e,
                                      LatencyTunerProfile_SecondOrder,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    // Zero steady-state bias (Type 2), band = target +- 5 ms >> residuals.
    CHECK(result.final_latency_ms > 195.0);
    CHECK(result.final_latency_ms < 205.0);
    // Warp must actually compensate the drift (5e-5), without large overshoot.
    CHECK(result.max_scaling_deviation > 3e-5);
    CHECK(result.max_scaling_deviation < 2e-4);
}

TEST(latency_tuner_second_order, e2e_jitter) {
    // White measurement noise only: injected true-latency stddev ~0.34 ms,
    // so a +-10 ms band is > 25 sigma. Warp wander sigma_u ~= 2.3e-4;
    // its max over 20000 epochs stays well under 3e-3 (and far from clamp).
    SimConfig sim;
    sim.jitter_stddev_ms = 10.0;
    sim.seed = 11;

    SimResult result = run_simulation(LatencyTunerBackend_E2e,
                                      LatencyTunerProfile_SecondOrder,
                                      200 * Ms, 200 * Ms, sim, 20000);

    CHECK(!result.bounds_violated);
    CHECK(result.final_latency_ms > 190.0);
    CHECK(result.final_latency_ms < 210.0);
    CHECK(result.max_scaling_deviation < 3e-3);
}

TEST(latency_tuner_second_order, e2e_drift_plus_jitter) {
    SimConfig sim;
    sim.drift_ppm = 50.0;
    sim.jitter_stddev_ms = 10.0;
    sim.seed = 12;

    SimResult result = run_simulation(LatencyTunerBackend_E2e,
                                      LatencyTunerProfile_SecondOrder,
                                      200 * Ms, 200 * Ms, sim, 30000);

    CHECK(!result.bounds_violated);
    CHECK(result.final_latency_ms > 190.0);
    CHECK(result.final_latency_ms < 210.0);
}

TEST(latency_tuner_second_order, multi_receiver_convergence) {
    // Two second-order tuners starting +-20 ms off target under the same
    // drift: both integrals drive bias to zero, so both latencies converge
    // to the target and the inter-receiver skew collapses. 150 s = 5 T_I.
    const core::nanoseconds_t target = 200 * Ms;
    const core::nanoseconds_t tolerance = 200 * Ms;

    LatencyConfig config1 =
        make_e2e_config(LatencyTunerProfile_SecondOrder, target, tolerance);
    LatencyConfig config2 =
        make_e2e_config(LatencyTunerProfile_SecondOrder, target, tolerance);
    TestTuner tuner1(config1);
    TestTuner tuner2(config2);
    CHECK(tuner1.is_valid());
    CHECK(tuner2.is_valid());

    double latency1_ms = 220.0;
    double latency2_ms = 180.0;
    const double step_ms = 5.0;
    const double drift_ppm = 30.0;
    const double drift_per_step = drift_ppm * step_ms / 1e6;

    float scaling1 = 1.0f;
    float scaling2 = 1.0f;

    for (size_t i = 0; i < 30000; i++) {
        latency1_ms += drift_per_step;
        latency2_ms += drift_per_step;

        core::nanoseconds_t lat1_ns = (core::nanoseconds_t)(latency1_ms * (double)Ms);
        core::nanoseconds_t lat2_ns = (core::nanoseconds_t)(latency2_ms * (double)Ms);

        step_e2e(tuner1, lat1_ns);
        step_e2e(tuner2, lat2_ns);

        float s1 = tuner1.fetch_scaling();
        float s2 = tuner2.fetch_scaling();

        if (s1 > 0) {
            scaling1 = s1;
            latency1_ms -= ((double)s1 - 1.0) * step_ms;
        }
        if (s2 > 0) {
            scaling2 = s2;
            latency2_ms -= ((double)s2 - 1.0) * step_ms;
        }
    }

    // Zero-bias convergence: both latencies at target within +-2 ms.
    CHECK(fabs(latency1_ms - 200.0) < 2.0);
    CHECK(fabs(latency2_ms - 200.0) < 2.0);

    // Inter-receiver skew collapses (the multiroom property).
    CHECK(fabs(latency1_ms - latency2_ms) < 0.5);

    // Both warps compensate the same drift.
    DOUBLES_EQUAL((double)scaling1, (double)scaling2, 1e-4);
}

} // namespace audio
} // namespace roc
