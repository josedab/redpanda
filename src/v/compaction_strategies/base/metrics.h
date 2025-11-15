// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/metrics.hh>

#include <cstddef>

namespace compaction_strategies::metrics {

// Scheduling metrics
class scheduling_metrics {
public:
    scheduling_metrics();

    void record_compaction_score(double score);
    void increment_decisions_made();
    void set_pending_compactions(size_t count);

private:
    seastar::metrics::metric_groups _metrics;
    seastar::metrics::histogram _compaction_score_distribution;
    size_t _compaction_decisions_made{0};
    size_t _pending_compactions{0};
};

// Execution metrics
class execution_metrics {
public:
    execution_metrics();

    void record_chunk_size(size_t size);
    void increment_pauses();
    void record_duration(std::chrono::milliseconds duration);

private:
    seastar::metrics::metric_groups _metrics;
    seastar::metrics::histogram _incremental_chunk_size;
    size_t _compaction_pauses{0};
    seastar::metrics::histogram _compaction_duration;
};

// Effectiveness metrics
class effectiveness_metrics {
public:
    effectiveness_metrics();

    void set_dead_data_ratio(double ratio);
    void set_space_amplification(double factor);
    void set_write_amplification(double factor);

private:
    seastar::metrics::metric_groups _metrics;
    double _dead_data_ratio{0.0};
    double _space_amplification{1.0};
    double _write_amplification{1.0};
};

// Strategy metrics
class strategy_metrics {
public:
    strategy_metrics();

    void increment_strategy_changes();
    void record_effectiveness_score(double score);

private:
    seastar::metrics::metric_groups _metrics;
    size_t _strategy_changes{0};
    seastar::metrics::histogram _strategy_effectiveness_score;
};

// Combined metrics for advanced compaction
class advanced_compaction_metrics {
public:
    advanced_compaction_metrics();

    scheduling_metrics& scheduling() { return _scheduling; }
    execution_metrics& execution() { return _execution; }
    effectiveness_metrics& effectiveness() { return _effectiveness; }
    strategy_metrics& strategy() { return _strategy; }

private:
    scheduling_metrics _scheduling;
    execution_metrics _execution;
    effectiveness_metrics _effectiveness;
    strategy_metrics _strategy;
};

} // namespace compaction_strategies::metrics
