load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "tracing",
    srcs = [
        "span.cc",
        "tracer.cc",
    ],
    hdrs = [
        "kafka_api_tracer.h",
        "raft_tracer.h",
        "span.h",
        "span_recorder.h",
        "trace_context.h",
        "tracer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/strings",
        "@seastar",
    ],
)
