load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//visibility:public",
    ],
)

redpanda_cc_library(
    name = "service_registry",
    hdrs = [
        "service_registry.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "load_balancer",
    hdrs = [
        "load_balancer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":service_registry",
        "//src/v/net",
        "@seastar",
    ],
)
