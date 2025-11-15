load("//bazel:build.bzl", "redpanda_cc_library")

# SQL Parser Library
redpanda_cc_library(
    name = "sql_parser",
    srcs = [
        "parser/sql_parser.cc",
        "parser/tokenizer.cc",
    ],
    hdrs = [
        "parser/ast.h",
        "parser/sql_parser.h",
        "parser/tokenizer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

# SQL Planner Library
redpanda_cc_library(
    name = "sql_planner",
    srcs = [
        "planner/query_planner.cc",
    ],
    hdrs = [
        "planner/logical_plan.h",
        "planner/physical_plan.h",
        "planner/query_planner.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":sql_parser",
        "//src/v/model",
        "@seastar",
    ],
)

# SQL Executor Library
redpanda_cc_library(
    name = "sql_executor",
    srcs = [
        "executor/operators.cc",
    ],
    hdrs = [
        "executor/operators.h",
        "executor/physical_operator.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

# Materialized View Library
redpanda_cc_library(
    name = "sql_materialized_view",
    srcs = [
        "materialized_view/view_manager.cc",
    ],
    hdrs = [
        "materialized_view/view_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":sql_executor",
        ":sql_planner",
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

# SQL Service Library
redpanda_cc_library(
    name = "sql_service",
    srcs = [
        "sql_service.cc",
    ],
    hdrs = [
        "sql_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":sql_materialized_view",
        ":sql_parser",
        ":sql_planner",
        "@seastar",
    ],
)

# Main SQL Library (aggregates all components)
redpanda_cc_library(
    name = "sql",
    visibility = ["//visibility:public"],
    deps = [
        ":sql_executor",
        ":sql_materialized_view",
        ":sql_parser",
        ":sql_planner",
        ":sql_service",
    ],
)
