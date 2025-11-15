# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

load("//bazel:build.bzl", "redpanda_cc_library", "redpanda_cc_test")

redpanda_cc_library(
    name = "cdc",
    srcs = [
        "cdc_source.cc",
        "postgres_cdc_source.cc",
        "mysql_cdc_source.cc",
        "schema_manager.cc",
        "transform_engine.cc",
        "state_manager.cc",
        "admin_api.cc",
    ],
    hdrs = [
        "types.h",
        "cdc_source.h",
        "postgres_cdc_source.h",
        "mysql_cdc_source.h",
        "schema_manager.h",
        "transform_engine.h",
        "state_manager.h",
        "admin_api.h",
    ],
    include_prefix = "cdc",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/json",
        "@seastar",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_test(
    name = "cdc_test",
    srcs = [
        "tests/cdc_source_test.cc",
        "tests/schema_manager_test.cc",
        "tests/transform_engine_test.cc",
    ],
    deps = [
        ":cdc",
        "@seastar//:testing",
    ],
)
