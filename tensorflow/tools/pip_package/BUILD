# Description:
#  Tools for building the TensorFlow pip package.

load("//tensorflow:tensorflow.bzl", "filegroup_as_file", "if_windows", "transitive_hdrs")
load("//third_party/mkl:build_defs.bzl", "if_mkl", "if_mkl_ml")
load("@local_config_cuda//cuda:build_defs.bzl", "if_cuda")
load("@local_config_syslibs//:build_defs.bzl", "if_not_system_lib")
load("//tensorflow/core/platform:build_config_root.bzl", "tf_additional_license_deps")
load("//third_party/ngraph:build_defs.bzl", "if_ngraph")

package(default_visibility = ["//visibility:private"])

# This returns a list of headers of all public header libraries (e.g.,
# framework, lib), and all of the transitive dependencies of those
# public headers.  Not all of the headers returned by the filegroup
# are public (e.g., internal headers that are included by public
# headers), but the internal headers need to be packaged in the
# pip_package for the public headers to be properly included.
#
# Public headers are therefore defined by those that are both:
#
# 1) "publicly visible" as defined by bazel
# 2) Have documentation.
#
# This matches the policy of "public" for our python API.
transitive_hdrs(
    name = "included_headers",
    deps = [
        "//tensorflow/c/experimental:network",
        "//tensorflow/compiler/tf2xla:xla_compiled_cpu_function",
        "//tensorflow/compiler/mlir:mlir_graph_optimization_pass",
        "//tensorflow/core:core_cpu",
        "//tensorflow/core:framework",
        "//tensorflow/core:lib",
        "//tensorflow/core:protos_all_cc",
        "//tensorflow/core:stream_executor",
        "//tensorflow/cc/saved_model:loader",
        "//tensorflow/cc/saved_model:reader",
        "//tensorflow/cc/saved_model:bundle_v2",
        # WARNING: None of the C/C++ code under python/ has any API guarantees, and TF team
        # reserves the right to change APIs and other header-level interfaces.  If your custom
        # op uses these headers, it may break when users upgrade their version of tensorflow.
        # NOTE(ebrevdo): None of the python symbols are exported by libtensorflow_framework.so.
        # Code that relies on these headers should dynamically link to
        # _pywrap_tensorflow_internal.so as well.
        "//tensorflow/python:model_analyzer_lib",
        "//tensorflow/python:py_exception_registry",
        "//tensorflow/python:pybind11_absl",
        "//tensorflow/python:pybind11_lib",
        "//tensorflow/python:pybind11_status",
        "//tensorflow/python:pybind11_proto",
        "//tensorflow/python:kernel_registry",
        "//tensorflow/python:cpp_python_util",
        "//tensorflow/python:py_func_lib",
        "//tensorflow/python:py_seq_tensor",
        "//tensorflow/python:py_util",
        "//tensorflow/python:py_record_reader_lib",
        "//tensorflow/python:python_op_gen",
        "//tensorflow/python:tf_session_helper",
        "//third_party/eigen3",
    ] + if_cuda([
        "@local_config_cuda//cuda:cuda_headers",
    ]),
)

py_binary(
    name = "simple_console",
    srcs = ["simple_console.py"],
    srcs_version = "PY2AND3",
    deps = ["//tensorflow:tensorflow_py"],
)

filegroup_as_file(
    name = "xla_compiled_cpu_runtime_srcs.txt",
    dep = "//tensorflow/compiler/tf2xla:xla_compiled_cpu_runtime_srcs",
)

filegroup(
    name = "xla_cmake",
    srcs = ["xla_build/CMakeLists.txt"],
)

# Add dynamic kernel dso files here.
DYNAMIC_LOADED_KERNELS = [
    "//tensorflow/core/kernels:libtfkernel_sobol_op.so",
]

COMMON_PIP_DEPS = [
    ":licenses",
    "MANIFEST.in",
    "README",
    "setup.py",
    ":included_headers",
    ":xla_compiled_cpu_runtime_srcs.txt_file",
    ":xla_cmake",
    "//tensorflow:tensorflow_py",
    "//tensorflow/compiler/tf2xla:xla_compiled_cpu_runtime_hdrs",
    "//tensorflow/compiler/tf2xla:xla_compiled_cpu_runtime_srcs",
    "//tensorflow/compiler/mlir/tensorflow:gen_mlir_passthrough_op_py",
    "//tensorflow/core:protos_all_proto_srcs",
    "//tensorflow/examples/saved_model/integration_tests:mnist_util",
    "//tensorflow/lite/python/testdata:interpreter_test_data",
    "//tensorflow/lite/python:tflite_convert",
    "//tensorflow/lite/toco/python:toco_from_protos",
    "//tensorflow/python/autograph/core:test_lib",
    "//tensorflow/python/autograph/impl/testing:pybind_for_testing",
    "//tensorflow/python/autograph/pyct/testing",
    "//tensorflow/python/autograph/pyct/common_transformers:common_transformers",
    "//tensorflow/python/compiler:compiler",
    "//tensorflow/python:cond_v2",
    "//tensorflow/python:distributed_framework_test_lib",
    "//tensorflow/python/distribute:distribute_test_lib_pip",
    "//tensorflow/python:loss_scale",
    "//tensorflow/python:loss_scale_optimizer",
    "//tensorflow/python:memory_checker",
    "//tensorflow/python:meta_graph_testdata",
    "//tensorflow/python:util_example_parser_configuration",
    "//tensorflow/python/data/benchmarks:benchmark_base",
    "//tensorflow/python/data/experimental/kernel_tests/serialization:dataset_serialization_test_base",
    "//tensorflow/python/data/experimental/kernel_tests:reader_dataset_ops_test_base",
    "//tensorflow/python/data/experimental/kernel_tests:stats_dataset_test_base",
    "//tensorflow/python/data/experimental/ops:testing",
    "//tensorflow/python/data/experimental/service:server_lib",
    "//tensorflow/python/data/kernel_tests:test_base",
    "//tensorflow/python/debug:debug_pip",
    "//tensorflow/python/distribute:combinations",
    "//tensorflow/python/distribute:multi_process_runner",
    "//tensorflow/python/eager:eager_pip",
    "//tensorflow/python/keras:combinations",
    "//tensorflow/python/keras/layers/preprocessing:preprocessing_test_utils",
    "//tensorflow/python/keras/distribute:distribute_strategy_test_lib",
    "//tensorflow/python/keras/distribute:multi_worker_testing_utils",
    "//tensorflow/python/keras/mixed_precision/experimental:test_util",
    "//tensorflow/python/keras/tests:model_subclassing_test_util",
    "//tensorflow/python/keras/tests:model_architectures",
    "//tensorflow/python/kernel_tests:cudnn_deterministic_base",
    "//tensorflow/python/kernel_tests:bias_op_base",
    "//tensorflow/python/kernel_tests/random:util",
    "//tensorflow/python/kernel_tests/signal:test_util",
    "//tensorflow/python/kernel_tests/testdata:self_adjoint_eig_op_test_files",
    "//tensorflow/python/profiler:traceme",
    "//tensorflow/python/saved_model:saved_model",
    "//tensorflow/python/tools:tools_pip",
    "//tensorflow/python/tools/api/generator:create_python_api",
    "//tensorflow/python/tpu",
    "//tensorflow/python:test_ops",
    "//tensorflow/python:while_v2",
    "//tensorflow/tools/common:public_api",
    "//tensorflow/tools/common:test_module1",
    "//tensorflow/tools/docs:doc_generator_visitor",
    "//tensorflow/tools/docs:generate_lib",
    "//tensorflow/tools/docs:parser",
    "//tensorflow/tools/docs:py_guide_parser",
]

# On Windows, python binary is a zip file of runfiles tree.
# Add everything to its data dependency for generating a runfiles tree
# for building the pip package on Windows.
py_binary(
    name = "simple_console_for_windows",
    srcs = ["simple_console_for_windows.py"],
    data = COMMON_PIP_DEPS + [
        "//tensorflow/python:pywrap_tensorflow_import_lib_file",
    ],
    srcs_version = "PY2AND3",
    deps = ["//tensorflow:tensorflow_py"],
)

filegroup(
    name = "licenses",
    data = [
        "//:LICENSE",
        "//third_party/eigen3:LICENSE",
        "//third_party/fft2d:LICENSE",
        "//third_party/hadoop:LICENSE.txt",
        "//third_party/icu/data:LICENSE",
        "@ruy//:LICENSE",
        "@arm_neon_2_x86_sse//:LICENSE",
        "@astunparse_archive//:LICENSE",
        "@astor_archive//:LICENSE",
        "@boringssl//:LICENSE",
        "@com_google_absl//:LICENSE",
        "@com_google_protobuf//:LICENSE",
        "@com_googlesource_code_re2//:LICENSE",
        "@curl//:COPYING",
        "@dill_archive//:LICENSE",
        "@dlpack//:LICENSE",
        "@double_conversion//:LICENSE",
        "@eigen_archive//:COPYING.MPL2",
        "@enum34_archive//:LICENSE",
        "@farmhash_archive//:COPYING",
        "@fft2d//:readme2d.txt",
        "@flatbuffers//:LICENSE.txt",
        "@functools32_archive//:LICENSE",
        "@gast_archive//:PKG-INFO",
        "@gemmlowp//:LICENSE",
        "@gif//:COPYING",
        "@highwayhash//:LICENSE",
        "@icu//:icu4c/LICENSE",
        "@kissfft//:COPYING",
        "@libjpeg_turbo//:LICENSE.md",
        "@llvm-project//llvm:LICENSE.TXT",
        "@llvm-project//mlir:LICENSE.TXT",
        "@lmdb//:LICENSE",
        "@local_config_sycl//sycl:LICENSE.text",
        "@local_config_tensorrt//:LICENSE",
        "@nasm//:LICENSE",
        "@nsync//:LICENSE",
        "@opt_einsum_archive//:LICENSE",
        "@org_python_pypi_backports_weakref//:LICENSE",
        "@pasta//:LICENSE",
        "@png//:LICENSE",
        "@six_archive//:LICENSE",
        "@snappy//:COPYING",
        "@sobol_data//:LICENSE",
        "@tblib_archive//:LICENSE",
        "@termcolor_archive//:COPYING.txt",
        "@zlib//:zlib.h",
        "@clog//:LICENSE",
        "@cpuinfo//:LICENSE",
    ] + select({
        "//tensorflow:android": [],
        "//tensorflow:ios": [],
        "//tensorflow:linux_s390x": [],
        "//tensorflow:windows": [],
        "//tensorflow:no_prisma_support": [],
        "//tensorflow:no_aws_support": [],
        "//conditions:default": [
            "@aws//:LICENSE",
            "@aws-c-common//:LICENSE",
            "@aws-c-event-stream//:LICENSE",
            "@aws-checksums//:LICENSE",
        ],
    }) + select({
        "//tensorflow:android": [],
        "//tensorflow:ios": [],
        "//tensorflow:linux_s390x": [],
        "//tensorflow:windows": [],
        "//tensorflow:no_gcp_support": [],
        "//conditions:default": [
            "@com_github_googlecloudplatform_google_cloud_cpp//:LICENSE",
        ],
    }) + select({
        "//tensorflow/core/kernels:xsmm": [
            "@libxsmm_archive//:LICENSE.md",
        ],
        "//conditions:default": [],
    }) + select({
        "//tensorflow:with_numa_support": [
            "@hwloc//:COPYING",
        ],
        "//conditions:default": [],
    }) + if_cuda([
        "@cub_archive//:LICENSE.TXT",
        "@local_config_nccl//:LICENSE",
    ]) + if_mkl([
        "//third_party/mkl:LICENSE",
        "//third_party/mkl_dnn:LICENSE",
    ]) + if_not_system_lib(
        "absl_py",
        [
            "@absl_py//absl:LICENSE",
            "@absl_py//absl/logging:LICENSE",
            "@absl_py//absl/flags:LICENSE",
            "@absl_py//absl/testing:LICENSE",
            "@absl_py//absl/third_party/unittest3_backport:LICENSE",
        ],
    ) + if_not_system_lib(
        "com_github_grpc_grpc",
        [
            "@com_github_grpc_grpc//:LICENSE",
            "@com_github_grpc_grpc//third_party/address_sorting:LICENSE",
        ],
    ) + if_ngraph([
        "@ngraph//:LICENSE",
        "@ngraph_tf//:LICENSE",
        "@nlohmann_json_lib//:LICENSE.MIT",
        "@tbb//:LICENSE",
    ]) + tf_additional_license_deps(),
)

sh_binary(
    name = "build_pip_package",
    srcs = ["build_pip_package.sh"],
    data = COMMON_PIP_DEPS +
           select({
               "//tensorflow:windows": [
                   ":simple_console_for_windows",
               ],
               "//conditions:default": [
                   ":simple_console",
               ],
           }) +
           select({
               "//tensorflow:dynamic_loaded_kernels": DYNAMIC_LOADED_KERNELS,
               "//conditions:default": [],
           }) + if_mkl_ml(["//third_party/mkl:intel_binary_blob"]),
)

# A genrule for generating a marker file for the pip package on Windows
#
# This only works on Windows, because :simple_console_for_windows is a
# python zip file containing everything we need for building the pip package.
# However, on other platforms, due to https://github.com/bazelbuild/bazel/issues/4223,
# when C++ extensions change, this generule doesn't rebuild.
genrule(
    name = "win_pip_package_marker",
    srcs = if_windows([
        ":build_pip_package",
        ":simple_console_for_windows",
    ]),
    outs = ["win_pip_package_marker_file"],
    cmd = select({
        "//conditions:default": "touch $@",
        "//tensorflow:windows": "md5sum $(locations :build_pip_package) $(locations :simple_console_for_windows) > $@",
    }),
    visibility = ["//visibility:public"],
)
