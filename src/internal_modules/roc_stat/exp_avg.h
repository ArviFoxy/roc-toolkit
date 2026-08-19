/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_stat/exp_avg.h
//! @brief Bias-corrected exponential average.

#ifndef ROC_STAT_EXP_AVG_H_
#define ROC_STAT_EXP_AVG_H_

namespace roc {
namespace stat {

//! Bias-corrected exponential average.
//!
//! Keeps an exponential sum of the samples and the weight accumulated by that
//! sum. Both start at zero and advance together, and the estimate is their
//! ratio, so the exponential weights are normalized by however much weight has
//! arrived so far. The estimate is thus the exponentially weighted average of
//! the samples seen so far, at every point of the sequence: there is no
//! starting bias towards zero, and none towards the first sample either.
//!
//! A plain exponential average seeded from zero keeps the seed with weight
//! (1 - alpha)^n, which holds the estimate below the average of the samples for
//! about 1 / alpha updates. Seeding from the first sample removes the pull
//! towards zero, but then that one sample dominates for the same span.
//!
//! The accumulated weight converges to one, after which the estimate is the
//! same as a plain exponential average.
class ExpAvg {
public:
    //! Initialize empty.
    ExpAvg()
        : raw_(0)
        , weight_(0) {
    }

    //! Check whether at least one sample was added.
    bool has() const {
        return weight_ > 0;
    }

    //! Get current estimate.
    //! @pre
    //!  has() should return true.
    double get() const {
        return raw_ / weight_;
    }

    //! Get accumulated weight, in [0; 1]; converges to one.
    double weight() const {
        return weight_;
    }

    //! Add sample @p x with discount factor @p alpha from (0; 1].
    void update(double alpha, double x) {
        raw_ += alpha * (x - raw_);
        weight_ += alpha * (1 - weight_);
    }

private:
    double raw_;
    double weight_;
};

} // namespace stat
} // namespace roc

#endif // ROC_STAT_EXP_AVG_H_
