load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "observability",
    hdrs = [
        "observability_config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/observability/logging",
        "//src/v/observability/tracing",
    ],
)
