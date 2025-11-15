// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction_strategies/base/metrics.h"

namespace compaction_strategies::metrics {

scheduling_metrics::scheduling_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group(
      "compaction_scheduling",
      {
        sm::make_histogram(
          "compaction_score_distribution",
          [this] { return _compaction_score_distribution; },
          sm::description("Distribution of compaction scores")),
        sm::make_counter(
          "compaction_decisions_made",
          [this] { return _compaction_decisions_made; },
          sm::description("Total compaction decisions made")),
        sm::make_gauge(
          "pending_compactions",
          [this] { return _pending_compactions; },
          sm::description("Number of pending compactions")),
      });
}

void scheduling_metrics::record_compaction_score(double score) {
    _compaction_score_distribution.sample(
      static_cast<seastar::metrics::histogram::duration>(score * 1000));
}

void scheduling_metrics::increment_decisions_made() {
    _compaction_decisions_made++;
}

void scheduling_metrics::set_pending_compactions(size_t count) {
    _pending_compactions = count;
}

execution_metrics::execution_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group(
      "compaction_execution",
      {
        sm::make_histogram(
          "incremental_chunk_size",
          [this] { return _incremental_chunk_size; },
          sm::description("Size of incremental compaction chunks")),
        sm::make_counter(
          "compaction_pauses",
          [this] { return _compaction_pauses; },
          sm::description("Number of compaction pauses")),
        sm::make_histogram(
          "compaction_duration",
          [this] { return _compaction_duration; },
          sm::description("Duration of compactions")),
      });
}

void execution_metrics::record_chunk_size(size_t size) {
    _incremental_chunk_size.sample(
      static_cast<seastar::metrics::histogram::duration>(size));
}

void execution_metrics::increment_pauses() { _compaction_pauses++; }

void execution_metrics::record_duration(std::chrono::milliseconds duration) {
    _compaction_duration.sample(
      std::chrono::duration_cast<seastar::metrics::histogram::duration>(
        duration));
}

effectiveness_metrics::effectiveness_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group(
      "compaction_effectiveness",
      {
        sm::make_gauge(
          "dead_data_ratio",
          [this] { return _dead_data_ratio; },
          sm::description("Ratio of dead data in segments")),
        sm::make_gauge(
          "space_amplification",
          [this] { return _space_amplification; },
          sm::description("Space amplification factor")),
        sm::make_gauge(
          "write_amplification",
          [this] { return _write_amplification; },
          sm::description("Write amplification factor")),
      });
}

void effectiveness_metrics::set_dead_data_ratio(double ratio) {
    _dead_data_ratio = ratio;
}

void effectiveness_metrics::set_space_amplification(double factor) {
    _space_amplification = factor;
}

void effectiveness_metrics::set_write_amplification(double factor) {
    _write_amplification = factor;
}

strategy_metrics::strategy_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group(
      "compaction_strategy",
      {
        sm::make_counter(
          "strategy_changes",
          [this] { return _strategy_changes; },
          sm::description("Number of strategy changes")),
        sm::make_histogram(
          "strategy_effectiveness_score",
          [this] { return _strategy_effectiveness_score; },
          sm::description("Effectiveness score of compaction strategies")),
      });
}

void strategy_metrics::increment_strategy_changes() { _strategy_changes++; }

void strategy_metrics::record_effectiveness_score(double score) {
    _strategy_effectiveness_score.sample(
      static_cast<seastar::metrics::histogram::duration>(score * 1000));
}

advanced_compaction_metrics::advanced_compaction_metrics()
  : _scheduling()
  , _execution()
  , _effectiveness()
  , _strategy() {}

} // namespace compaction_strategies::metrics
