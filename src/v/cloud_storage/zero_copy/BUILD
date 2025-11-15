load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "zero_copy",
    srcs = [
        "config.cc",
        "direct_io_reader.cc",
        "io_uring_reader.cc",
        "scatter_gather_buffer.cc",
        "zero_copy_decompressor.cc",
    ],
    hdrs = [
        "config.h",
        "direct_io_reader.h",
        "io_uring_reader.h",
        "scatter_gather_buffer.h",
        "zero_copy_decompressor.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/compression",
        "//src/v/ssx",
        "@liburing",
        "@seastar",
    ],
)
