#!/bin/sh

yum makecache

# we need LLVM >= 3.9 for onig_sys/bindgen

yum install -y centos-release-scl
yum install -y llvm-toolset-7
yum install -y perl-core

# we need GCC >= 4.9 for grpcio
yum install -y devtoolset-7
