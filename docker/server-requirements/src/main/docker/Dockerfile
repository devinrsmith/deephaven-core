ARG BASE
FROM $BASE as install
COPY install.sh install.sh
ARG TARGETARCH
RUN set -eux; \
    ./install.sh; \
    rm ./install.sh

FROM $BASE
COPY --from=install / /
