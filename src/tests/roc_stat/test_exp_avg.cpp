/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "test_harness.h"

#include "roc_core/macro_helpers.h"
#include "roc_core/stddefs.h"
#include "roc_stat/exp_avg.h"

namespace roc {
namespace stat {

namespace {

const double Epsilon = 1e-12;

// Exponentially weighted average of x[0] .. x[n-1], with the weights
// normalized by their own sum. Sample x[i] has weight alpha*(1-alpha)^(n-1-i),
// and the weights sum up to 1-(1-alpha)^n.
double reference_avg(double alpha, const double* x, size_t n) {
    double sum = 0, weight = 0;

    for (size_t i = 0; i < n; i++) {
        const double w = alpha * pow(1 - alpha, (double)(n - 1 - i));
        sum += w * x[i];
        weight += w;
    }

    return sum / weight;
}

} // namespace

TEST_GROUP(exp_avg) { };

TEST(exp_avg, empty) {
    ExpAvg comp;

    CHECK(!comp.has());
}

TEST(exp_avg, single_sample) {
    // The only sample carries all the accumulated weight, whatever alpha is.
    const double alphas[] = { 0.001, 0.1, 0.5, 1.0 };

    for (size_t i = 0; i < ROC_ARRAY_SIZE(alphas); i++) {
        ExpAvg comp;
        comp.update(alphas[i], 42.0);

        CHECK(comp.has());
        DOUBLES_EQUAL(42.0, comp.get(), Epsilon);
    }
}

TEST(exp_avg, two_samples) {
    const double alpha = 0.25;

    ExpAvg comp;
    comp.update(alpha, 10.0);
    comp.update(alpha, 20.0);

    // ((1-a)*x1 + x2) / ((1-a) + 1)
    const double expected = ((1 - alpha) * 10.0 + 20.0) / ((1 - alpha) + 1);

    CHECK(comp.has());
    DOUBLES_EQUAL(expected, comp.get(), Epsilon);
}

TEST(exp_avg, constant_input) {
    // Constant input reads back as that constant from the very first update,
    // instead of climbing towards it for about 1/alpha updates.
    const double alpha = 0.01;

    ExpAvg comp;

    for (size_t i = 0; i < 1000; i++) {
        comp.update(alpha, -3.5);
        DOUBLES_EQUAL(-3.5, comp.get(), Epsilon);
    }
}

TEST(exp_avg, matches_reference) {
    const double alpha = 0.3;
    const double x[] = { 1.0, -2.0, 5.5, 0.0, 3.25, -7.75, 2.0 };

    ExpAvg comp;

    for (size_t n = 0; n < ROC_ARRAY_SIZE(x); n++) {
        comp.update(alpha, x[n]);

        CHECK(comp.has());
        DOUBLES_EQUAL(reference_avg(alpha, x, n + 1), comp.get(), Epsilon);
    }
}

TEST(exp_avg, converges_to_recent_samples) {
    // After a step, the estimate leaves the old level and approaches the new
    // one, since the weight of the old samples decays.
    const double alpha = 0.1;

    ExpAvg comp;

    for (size_t i = 0; i < 500; i++) {
        comp.update(alpha, 1.0);
    }
    DOUBLES_EQUAL(1.0, comp.get(), 1e-9);

    for (size_t i = 0; i < 500; i++) {
        comp.update(alpha, 2.0);
    }
    DOUBLES_EQUAL(2.0, comp.get(), 1e-9);
}

} // namespace stat
} // namespace roc
