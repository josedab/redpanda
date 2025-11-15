load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//visibility:public",
    ],
)

redpanda_cc_library(
    name = "metadata_service",
    hdrs = [
        "metadata_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/kafka/protocol",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "consensus_metadata_service",
    hdrs = [
        "consensus_metadata_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metadata_service",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)
