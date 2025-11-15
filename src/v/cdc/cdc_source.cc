// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cdc/cdc_source.h"
#include "cdc/postgres_cdc_source.h"
#include "cdc/mysql_cdc_source.h"

#include <seastar/util/log.hh>

namespace redpanda::cdc {

static ss::logger logger{"cdc_source"};

std::unique_ptr<cdc_source> cdc_source_factory::create(const ss::sstring& type) {
    if (type == "postgresql") {
        return std::make_unique<postgres_cdc_source>();
    } else if (type == "mysql") {
        return std::make_unique<mysql_cdc_source>();
    }

    throw std::invalid_argument(
        fmt::format("Unknown CDC source type: {}", type));
}

} // namespace redpanda::cdc
