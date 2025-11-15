load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logging",
    hdrs = [
        "log_query_engine.h",
        "structured_logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/json",
        "//src/v/observability/tracing",
        "@seastar",
    ],
)
