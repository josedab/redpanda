load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/storage_service/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "storage_service",
    hdrs = [
        "storage_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cloud_storage_service",
    hdrs = [
        "cloud_storage_service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":storage_service",
        "//src/v/cloud_storage_clients",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)
