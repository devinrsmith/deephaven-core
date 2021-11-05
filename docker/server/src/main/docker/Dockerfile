FROM deephaven/server-requirements:local-build as install
COPY install.sh install.sh
ARG TARGETARCH
RUN ./install.sh

FROM deephaven/server-requirements:local-build
COPY --from=install / /
