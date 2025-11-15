load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/compute/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "compute_cache",
    hdrs = [
        "compute_cache.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "compute_node",
    hdrs = [
        "compute_node.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":compute_cache",
        "//src/v/kafka/server/handlers",
        "//src/v/metadata_service",
        "//src/v/model",
        "//src/v/storage_service",
        "@seastar",
    ],
)
