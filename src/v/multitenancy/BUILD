load("//bazel:build.bzl", "redpanda_cc_library", "redpanda_cc_test")

redpanda_cc_library(
    name = "types",
    hdrs = [
        "types.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "token_bucket",
    hdrs = [
        "token_bucket.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cpu_scheduler",
    srcs = [
        "cpu_scheduler.cc",
    ],
    hdrs = [
        "cpu_scheduler.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":token_bucket",
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_manager",
    srcs = [
        "memory_manager.cc",
    ],
    hdrs = [
        "memory_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "io_scheduler",
    srcs = [
        "io_scheduler.cc",
    ],
    hdrs = [
        "io_scheduler.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":token_bucket",
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "network_shaper",
    srcs = [
        "network_shaper.cc",
    ],
    hdrs = [
        "network_shaper.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":token_bucket",
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "resource_domain",
    srcs = [
        "resource_domain.cc",
    ],
    hdrs = [
        "resource_domain.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cpu_scheduler",
        ":io_scheduler",
        ":memory_manager",
        ":network_shaper",
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "encryption_manager",
    srcs = [
        "encryption_manager.cc",
    ],
    hdrs = [
        "encryption_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tenant",
    srcs = [
        "tenant.cc",
    ],
    hdrs = [
        "tenant.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":resource_domain",
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "tenant_manager",
    srcs = [
        "tenant_manager.cc",
    ],
    hdrs = [
        "tenant_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":encryption_manager",
        ":tenant",
        ":types",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "config",
    srcs = [
        "config.cc",
    ],
    hdrs = [
        "config.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "multitenancy",
    visibility = ["//visibility:public"],
    deps = [
        ":config",
        ":cpu_scheduler",
        ":encryption_manager",
        ":fwd",
        ":io_scheduler",
        ":memory_manager",
        ":network_shaper",
        ":resource_domain",
        ":tenant",
        ":tenant_manager",
        ":token_bucket",
        ":types",
    ],
)

# Tests

redpanda_cc_test(
    name = "tenant_test",
    timeout = "short",
    srcs = [
        "tests/tenant_test.cc",
    ],
    deps = [
        ":tenant",
        ":types",
        "@boost//:test",
    ],
)

redpanda_cc_test(
    name = "tenant_manager_test",
    timeout = "short",
    srcs = [
        "tests/tenant_manager_test.cc",
    ],
    deps = [
        ":tenant_manager",
        ":types",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_test(
    name = "token_bucket_test",
    timeout = "short",
    srcs = [
        "tests/token_bucket_test.cc",
    ],
    deps = [
        ":token_bucket",
        "@boost//:test",
        "@seastar",
    ],
)
