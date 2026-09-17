"""Conventional Cargo-shaped Rust targets for Walgit's workspace crates."""

load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_test")


def _merge(left, right):
    out = dict(left)
    out.update(right)
    return out


def walgit_rust_crate(
        name,
        crate_name,
        binaries = {},
        binary_deps = [],
        compile_data = [],
        crate_features = [],
        deps = [],
        proc_macro_deps = [],
        test_deps = [],
        test_proc_macro_deps = [],
        test_data_extra = [],
        test_env_extra = {},
        test_tags = {},
        integration_test_excludes = [],
        rustc_env = {},
        rustc_flags = [],
        edition = "2024"):
    """Defines one library, its binaries, unit test, and per-file integration tests."""
    package_files = native.glob(
        ["**"],
        exclude = ["BUILD.bazel", "target/**"],
        allow_empty = True,
    )
    native.filegroup(
        name = "package-files",
        srcs = package_files,
        visibility = ["//visibility:public"],
    )
    native.filegroup(
        name = "docs",
        srcs = native.glob(["**/*.md"], exclude = ["target/**"], allow_empty = True),
        visibility = ["//visibility:public"],
    )

    cargo_env = _merge({
        "CARGO_PKG_VERSION": "0.1.0",
        "WALGIT_BUILD_SHA": "bazel",
    }, rustc_env)
    library_root = native.glob(["src/lib.rs"], allow_empty = True)
    has_library = bool(library_root)

    if has_library:
        rust_library(
            name = name,
            crate_name = crate_name,
            crate_root = "src/lib.rs",
            srcs = native.glob(["src/**/*.rs"], allow_empty = False),
            crate_features = crate_features,
            compile_data = compile_data,
            edition = edition,
            deps = deps,
            proc_macro_deps = proc_macro_deps,
            rustc_env = cargo_env,
            rustc_flags = rustc_flags,
            version = "0.1.0",
            visibility = ["//visibility:public"],
        )
        rust_test(
            name = name + "-unit-tests",
            crate = ":" + name,
            compile_data = compile_data,
            edition = edition,
            deps = test_deps,
            proc_macro_deps = test_proc_macro_deps,
            data = test_data_extra,
            env = test_env_extra,
            tags = test_tags.get("unit", []),
        )

    binary_targets = []
    binary_env = dict(test_env_extra)
    for binary_name, crate_root in binaries.items():
        binary_target = ":" + binary_name
        binary_targets.append(binary_target)
        binary_env["CARGO_BIN_EXE_" + binary_name] = "$(rootpath %s)" % binary_target
        rust_binary(
            name = binary_name,
            crate_name = binary_name.replace("-", "_"),
            crate_root = crate_root,
            srcs = native.glob(
                ["src/**/*.rs"],
                exclude = binaries.values(),
                allow_empty = True,
            ) + [crate_root],
            edition = edition,
            deps = (([":" + name] if has_library else deps) + binary_deps),
            proc_macro_deps = ([] if has_library else proc_macro_deps),
            rustc_env = cargo_env,
            rustc_flags = rustc_flags,
            stamp = 0,
            visibility = ["//visibility:public"],
        )

    integration_deps = (([":" + name] if has_library else []) + deps + test_deps)
    all_test_sources = native.glob(["tests/**/*.rs"], allow_empty = True)
    integration_targets = []
    for test in native.glob(["tests/*.rs"], allow_empty = True):
        stem = test.removeprefix("tests/").removesuffix(".rs")
        if stem in integration_test_excludes:
            continue
        target = name + "-" + stem + "-test"
        tags = test_tags.get(stem, [])
        if "manual" not in tags:
            integration_targets.append(":" + target)
        rust_test(
            name = target,
            crate_name = (crate_name + "_" + stem).replace("-", "_"),
            crate_root = test,
            srcs = all_test_sources,
            edition = edition,
            deps = integration_deps,
            proc_macro_deps = proc_macro_deps + test_proc_macro_deps,
            data = binary_targets + test_data_extra,
            env = binary_env,
            rustc_env = _merge(cargo_env, {"CARGO_MANIFEST_DIR": native.package_name()}),
            rustc_flags = rustc_flags,
            tags = tags,
        )

    native.test_suite(
        name = "all-tests",
        visibility = ["//visibility:public"],
        tests = (([":" + name + "-unit-tests"] if has_library else []) + integration_targets),
    )
