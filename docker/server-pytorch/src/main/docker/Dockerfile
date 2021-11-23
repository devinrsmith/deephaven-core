FROM deephaven/server-base-python:local-build as install

COPY requirements.txt requirements.txt
RUN set -eux; \
    python3 -m pip install --no-cache-dir -r requirements.txt; \
    rm requirements.txt

FROM deephaven/server-base-python:local-build
COPY --from=install / /