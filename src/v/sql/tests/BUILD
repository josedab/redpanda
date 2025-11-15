load("//bazel:build.bzl", "redpanda_cc_test")

# SQL Parser Tests
redpanda_cc_test(
    name = "sql_parser_test",
    srcs = ["sql_parser_test.cc"],
    deps = [
        "//src/v/sql:sql_parser",
        "@boost//:test",
        "@seastar",
    ],
)

# SQL Planner Tests
redpanda_cc_test(
    name = "sql_planner_test",
    srcs = ["sql_planner_test.cc"],
    deps = [
        "//src/v/sql:sql_parser",
        "//src/v/sql:sql_planner",
        "@boost//:test",
        "@seastar",
    ],
)

# SQL Integration Tests
redpanda_cc_test(
    name = "sql_integration_test",
    srcs = ["sql_integration_test.cc"],
    deps = [
        "//src/v/sql:sql_service",
        "@boost//:test",
        "@seastar",
    ],
)

# Test suite
test_suite(
    name = "all_tests",
    tests = [
        ":sql_integration_test",
        ":sql_parser_test",
        ":sql_planner_test",
    ],
)
