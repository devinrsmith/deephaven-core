ARG BASE
FROM $BASE
WORKDIR /usr/src/app
COPY . .
ARG DEEPHAVEN_VERSION
RUN set -eux; \
    test -n "${DEEPHAVEN_VERSION}"; \
    python3.7 setup.py bdist_wheel
