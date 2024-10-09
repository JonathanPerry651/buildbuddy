load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testspanner",
    testonly = 1,
    srcs = [
        "testspanner.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testspanner",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_jackc_pgx_v5//:pgx",
        "@com_github_stretchr_testify//require",
        "@com_google_cloud_go_spanner//:spanner",
        "@com_google_cloud_go_spanner//admin/database/apiv1",
        "@com_google_cloud_go_spanner//admin/instance/apiv1",
    ],
)
