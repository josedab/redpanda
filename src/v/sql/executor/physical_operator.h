// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace redpanda::sql {

// Column-oriented data representation
class column_vector {
public:
    enum class type {
        int32,
        int64,
        float32,
        float64,
        string_val,
        bytes,
        timestamp,
        boolean
    };

    column_vector(type t, size_t capacity)
      : _type(t)
      , _count(0) {
        _data.reserve(capacity * type_size(t));
    }

    type get_type() const { return _type; }
    size_t size() const { return _count; }

    template<typename T>
    void append(T value) {
        auto* ptr = reinterpret_cast<uint8_t*>(&value);
        _data.insert(_data.end(), ptr, ptr + sizeof(T));
        _count++;
    }

    template<typename T>
    T get(size_t index) const {
        size_t offset = index * sizeof(T);
        return *reinterpret_cast<const T*>(_data.data() + offset);
    }

private:
    size_t type_size(type t) const {
        switch (t) {
        case type::int32:
        case type::float32:
            return 4;
        case type::int64:
        case type::float64:
        case type::timestamp:
            return 8;
        case type::boolean:
            return 1;
        default:
            return 8; // Pointer size for variable-length types
        }
    }

    type _type;
    std::vector<uint8_t> _data;
    size_t _count;
    std::optional<std::vector<bool>> _nulls;
};

// Batch of rows in columnar format
struct row_batch {
    std::vector<column_vector> columns;
    size_t row_count{0};

    row_batch() = default;
    explicit row_batch(size_t num_cols) {
        columns.reserve(num_cols);
    }
};

// Operator execution statistics
struct operator_stats {
    size_t rows_scanned{0};
    size_t rows_emitted{0};
    uint64_t cpu_time_us{0};
    uint64_t io_time_us{0};
    size_t memory_used{0};
};

// Base class for physical operators
class physical_operator {
public:
    virtual ~physical_operator() = default;

    // Open the operator and initialize resources
    virtual ss::future<> open() = 0;

    // Get next batch of rows
    virtual ss::future<std::optional<row_batch>> next() = 0;

    // Close the operator and release resources
    virtual ss::future<> close() = 0;

    // Get execution statistics
    virtual operator_stats get_stats() const = 0;
};

} // namespace redpanda::sql
