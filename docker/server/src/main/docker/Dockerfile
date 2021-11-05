FROM deephaven/deephaven-jpy-wheel:local-build as deephaven-jpy-wheel
FROM deephaven/deephaven-wheel:local-build as deephaven-wheel
FROM deephaven/deephaven2-wheel:local-build as deephaven2-wheel

FROM deephaven/server-requirements:local-build as install

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

ADD server.tar /opt/deephaven

FROM deephaven/server-requirements:local-build
COPY --from=install / /
