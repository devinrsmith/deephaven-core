ARG BASE

FROM deephaven/deephaven-jpy-wheel:local-build as deephaven-jpy-wheel
FROM deephaven/deephaven-wheel:local-build as deephaven-wheel
FROM deephaven/deephaven2-wheel:local-build as deephaven2-wheel
FROM $BASE as install

COPY common/ common/
ARG TARGETARCH
RUN set -eux; \
    ./common/server-requirements.sh; \
    rm -r ./common

COPY --from=deephaven-jpy-wheel /usr/src/app/dist /wheels
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /wheels/*.whl; \
    rm -rf /wheels

COPY --from=deephaven-wheel /usr/src/app/dist /wheels
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /wheels/*.whl; \
    rm -rf /wheels

COPY --from=deephaven2-wheel /usr/src/app/dist /wheels
RUN set -eux; \
    python3 -m pip install -q --no-cache-dir /wheels/*.whl; \
    rm -rf /wheels

ADD server/server.tar /opt/deephaven

FROM $BASE
COPY --from=install / /
HEALTHCHECK --interval=3s --retries=3 --timeout=11s CMD /bin/grpc_health_probe -addr=localhost:8080 -connect-timeout=10s || exit 1
ENTRYPOINT /opt/deephaven/grpc-api-server-native-0.6.0/bin/grpc-api-server-native