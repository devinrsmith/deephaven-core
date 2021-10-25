FROM ubuntu:20.04 as CPP_BUILD_TOOLS
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND="noninteractive" apt-get install -y wget git g++ cmake make build-essential libssl-dev; \
    rm -rf /var/lib/apt/lists/*

FROM CPP_BUILD_TOOLS as CPP_BUILD_DEPENDENCIES
COPY deephaven deephaven/
RUN set -eux; \
    mkdir -p /root/dhcpp/src /root/dhcpp/lib; \
    ./deephaven/client/build-dependencies.sh

FROM CPP_BUILD_DEPENDENCIES
COPY build-all build-all/
COPY tests tests/
COPY cpp-examples cpp-examples/
RUN set -eux; \
    mkdir build-all/build; \
    cd build-all/build; \
    CMAKE_PREFIX_PATH=/root/dhcpp/lib/abseil:/root/dhcpp/lib/cares:/root/dhcpp/lib/flatbuffers:/root/dhcpp/lib/gflags:/root/dhcpp/lib/protobuf:/root/dhcpp/lib/re2:/root/dhcpp/lib/zlib:/root/dhcpp/lib/grpc:/root/dhcpp/lib/arrow:/root/dhcpp/lib/deephaven \
    cmake -DCMAKE_BUILD_TYPE=Debug ..; \
    make -j8; \
    make install