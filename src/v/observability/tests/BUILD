load("//bazel:build.bzl", "redpanda_cc_unit_test")

redpanda_cc_unit_test(
    name = "tracing_test",
    timeout = "short",
    srcs = [
        "tracing_test.cc",
    ],
    deps = [
        "//src/v/observability/tracing",
        "@seastar",
        "@boost//:test",
    ],
)

redpanda_cc_unit_test(
    name = "metrics_test",
    timeout = "short",
    srcs = [
        "metrics_test.cc",
    ],
    deps = [
        "//src/v/metrics:partition_metrics",
        "//src/v/metrics:client_metrics",
        "//src/v/metrics:cost_tracker",
        "//src/v/model",
        "@seastar",
        "@boost//:test",
    ],
)
