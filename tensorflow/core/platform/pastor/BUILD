# Description:
# Pastor file system implementation.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

load("//tensorflow:tensorflow.bzl", "tf_cc_binary")
load("//tensorflow/core/platform:build_config.bzl", "tf_proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

tf_cc_binary(
    name = "libpastor_file_system_shared.so",
    srcs = [
        "pastor_file_system.cc",
        "pastor_file_system.h",
    ],
    linkshared = 1,
    deps = [
        ":pastor",
        "//tensorflow/core:framework_headers_lib",
    ]
)

cc_library(
    name = "pastor_file_system",
    srcs = [
        "pastor_file_system.cc",
    ],
    hdrs = [
        "pastor_file_system.h",
    ],
    deps = [
        ":pastor",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
    ],
)

cc_library( 
	name = "pastor",
	hdrs = glob([
		"data_plane/**/*.h",
		"helpers/*.h",
	]),
	srcs = glob([
		"data_plane/**/*.cpp",
		"helpers/*.cpp",
	]),
	defines = ["TF_BAZEL_BUILD"],
	deps = [
		":controller_service_cc_grpc",
		"//third_party/tbb:tbb",
		"//third_party/ctpl:ctpl",
		"@com_github_grpc_grpc//:grpc++",
		"@com_github_jbeder_yaml_cpp//:yaml-cpp",
	]
)

tf_proto_library(
    name = "controller_service_proto",
    srcs = ["protos/controller_service.proto"],
)

cc_grpc_library(
    name = "controller_service_cc_grpc",
    srcs = [":controller_service_proto"],
    grpc_only = True,
    deps = [":controller_service_proto_cc"],
)

#load("@com_github_grpc_grpc//bazel:grpc_build_system.bzl", "grpc_proto_library")

#cc_library( 
#    name = "test-pastor",
#    deps = [
#        "@com_github_grpc_grpc//:grpc++",
#        ":controller_service_cc_grpc",
#        #"@com_github_jbeder_yaml_cpp//:yaml-cpp",
#        #"//third_party/tbb:tbb",
#		#"//third_party/ctpl:ctpl",
#    ],
#)
#
#grpc_proto_library(
#	name = "controller_service_cc_grpc",
#	srcs = ["protos/controller_service.proto"],
#)