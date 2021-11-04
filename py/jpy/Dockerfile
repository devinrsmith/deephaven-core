FROM ghcr.io/devinrsmith/java as java_base

FROM ghcr.io/devinrsmith/java-python as runtime_reqs

FROM ghcr.io/devinrsmith/java-python-devel as build_reqs

FROM build_reqs as sources
WORKDIR /usr/src/app
COPY . .

FROM sources as build
ARG DEEPHAVEN_VERSION
RUN set -eux; \
    test -n "${DEEPHAVEN_VERSION}"; \
    python3.7 setup.py bdist_wheel
