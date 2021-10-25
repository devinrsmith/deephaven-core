FROM ubuntu:20.04 as CPP_BUILD_REQUIREMENTS
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND="noninteractive" apt-get install -y wget git g++ cmake make build-essential libssl-dev; \
    rm -rf /var/lib/apt/lists/*

FROM CPP_BUILD_REQUIREMENTS
COPY client client/
RUN set -eux; \
    mkdir -p /root/dhcpp/src /root/dhcpp/lib; \
    ./client/build-dependencies.sh