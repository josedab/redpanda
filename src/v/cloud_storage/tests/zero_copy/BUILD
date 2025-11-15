load("//bazel:build.bzl", "redpanda_cc_bench", "redpanda_cc_test")

redpanda_cc_test(
    name = "zero_copy_test",
    timeout = "short",
    srcs = [
        "zero_copy_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage/zero_copy",
        "//src/v/compression",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "zero_copy_benchmark",
    timeout = "moderate",
    srcs = [
        "zero_copy_benchmark.cc",
    ],
    deps = [
        "//src/v/cloud_storage/zero_copy",
        "//src/v/compression",
        "@seastar",
    ],
)
