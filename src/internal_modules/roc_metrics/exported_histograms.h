/*
 * Copyright (c) 2026 Roc Streaming authors
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

//! @file roc_metrics/exported_histograms.h
//! @brief The histograms created by exported_timing_histogram().

#ifndef ROC_METRICS_EXPORTED_HISTOGRAMS_H_
#define ROC_METRICS_EXPORTED_HISTOGRAMS_H_

#include "roc_metrics/atomic_histogram.h"

#include <map>
#include <string>
#include <vector>

namespace roc {
namespace metrics {

//! One histogram with the name and labels it is served under.
struct ExportedHistogram {
    //! Metric name.
    std::string name;
    //! Help text.
    std::string help;
    //! Label names and values.
    std::map<std::string, std::string> labels;
    //! The histogram; never freed.
    AtomicHistogram* histogram;
};

//! Snapshot of the list of exported histograms, in creation order.
std::vector<ExportedHistogram> exported_histograms();

} // namespace metrics
} // namespace roc

#endif // ROC_METRICS_EXPORTED_HISTOGRAMS_H_
