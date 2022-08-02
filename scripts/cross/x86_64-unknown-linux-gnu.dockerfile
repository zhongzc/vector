FROM docker.io/rustembedded/cross:x86_64-unknown-linux-gnu

COPY bootstrap-rhel.sh .
RUN ./bootstrap-rhel.sh

ENV LIBCLANG_PATH=/opt/rh/llvm-toolset-7/root/usr/lib64/ \
  LIBCLANG_STATIC_PATH=/opt/rh/llvm-toolset-7/root/usr/lib64/ \
  CLANG_PATH=/opt/rh/llvm-toolset-7/root/usr/bin/clang \
  CC=/opt/rh/devtoolset-7/root/usr/bin/gcc \
  CXX=/opt/rh/devtoolset-7/root/usr/bin/g++ \
  CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/opt/rh/devtoolset-7/root/usr/bin/gcc
