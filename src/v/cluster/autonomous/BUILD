load("//bazel:build.bzl", "redpanda_cc_library", "redpanda_cc_test")

redpanda_cc_library(
    name = "anomaly_detector",
    srcs = [
        "anomaly_detector.cc",
    ],
    hdrs = [
        "anomaly_detector.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:fwd",
        "//src/v/cluster:logger",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "predictive_scaler",
    srcs = [
        "predictive_scaler.cc",
    ],
    hdrs = [
        "predictive_scaler.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:fwd",
        "//src/v/cluster:logger",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "configuration_tuner",
    srcs = [
        "configuration_tuner.cc",
    ],
    hdrs = [
        "configuration_tuner.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:fwd",
        "//src/v/cluster:logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "self_healer",
    srcs = [
        "self_healer.cc",
    ],
    hdrs = [
        "self_healer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":anomaly_detector",
        "//src/v/cluster:fwd",
        "//src/v/cluster:logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "autonomous_rebalancer",
    srcs = [
        "autonomous_rebalancer.cc",
    ],
    hdrs = [
        "autonomous_rebalancer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cluster:fwd",
        "//src/v/cluster:logger",
        "//src/v/cluster:types",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "autonomous_controller",
    srcs = [
        "autonomous_controller.cc",
    ],
    hdrs = [
        "autonomous_controller.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":anomaly_detector",
        ":autonomous_rebalancer",
        ":configuration_tuner",
        ":predictive_scaler",
        ":self_healer",
        "//src/v/cluster:fwd",
        "//src/v/cluster:logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "autonomous",
    visibility = ["//visibility:public"],
    deps = [
        ":anomaly_detector",
        ":autonomous_controller",
        ":autonomous_rebalancer",
        ":configuration_tuner",
        ":predictive_scaler",
        ":self_healer",
    ],
)

redpanda_cc_test(
    name = "autonomous_operations_test",
    timeout = "short",
    srcs = [
        "tests/autonomous_operations_test.cc",
    ],
    deps = [
        ":autonomous",
        "//src/v/model",
        "@boost//:test",
        "@seastar",
    ],
)
