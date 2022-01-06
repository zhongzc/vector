#!/bin/bash
set -euo pipefail

# This script should be moved to a build.rs in `vrl/compiler`.
# However, currently the `vrl/compiler/src/precompiler` lib has a cyclic
# dependency on `vrl/compiler`, such that it's not possible to invoke `cargo`
# from the build script without a deadlock.

SCRIPT_DIR="$(cd $(dirname "$BASH_SOURCE[0]") && pwd)" &> /dev/null

PROFILE_ARG=""
if [ "$PROFILE" = "release" ]; then
  PROFILE_ARG="--release"
fi

PRECOMPILED_DIR="$SCRIPT_DIR/src/precompiled"
PRECOMPILED_TARGET_DIR="$PRECOMPILED_DIR/target/$TARGET/$PROFILE"

STD_DIR="$PRECOMPILED_DIR/std"
STD_TARGET_DIR="$STD_DIR/target/$TARGET/$PROFILE"

RUSTFLAGS="--emit=llvm-bc" RUSTC_BOOTSTRAP=1 cargo build --manifest-path="$STD_DIR/Cargo.toml" $PROFILE_ARG --lib --target $TARGET -Z build-std=std
RUSTFLAGS="--emit=llvm-bc" cargo build --manifest-path="$PRECOMPILED_DIR/Cargo.toml" $PROFILE_ARG --lib --target $TARGET

BC_FILES=$(ls "$STD_TARGET_DIR"/deps/*.bc "$PRECOMPILED_TARGET_DIR"/deps/*.bc | egrep -v -i "(panic_abort|proc_macro).*\.bc")

llvm-link $BC_FILES > "$PRECOMPILED_TARGET_DIR/precompiled.bc"