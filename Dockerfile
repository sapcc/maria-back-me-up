FROM keppel.eu-de-1.cloud.sap/ccloud-dockerhub-mirror/library/golang:1.22-alpine as builder
WORKDIR /go/src/github.com/sapcc/maria-back-me-up
RUN apk add --no-cache make git
COPY . /src
RUN make -C /src install PREFIX=/pkg GO_BUILDFLAGS='-mod vendor'

FROM keppel.eu-de-1.cloud.sap/ccloud-dockerhub-mirror/library/alpine:3.22.0
LABEL maintainer="Stefan Hipfel <stefan.hipfel@sap.com>"
LABEL source_repository="https://github.com/sapcc/maria-back-me-up"

ARG MYDUMPER_CMAKE_ARGS="-DWITH_ZSTD=ON"
ARG MYDUMPER_VERSION="v0.19.1-3"

# libressl-dev has problems with openssl-dev
ENV PACKAGES="openssh-client ca-certificates bash curl gzip openssl mysql-client " \
    LIB_PACKAGES="glib-dev mariadb-dev zlib-dev pcre-dev gcompat" \
    BUILD_PACKAGES="cmake build-base git" \
    MYDUMPER_BUILD_PATH="/opt/mydumper-src/"

ADD "https://github.com/mydumper/mydumper/archive/refs/tags/${MYDUMPER_VERSION}.tar.gz" /tmp/mydumper.tar.gz
RUN apk --no-cache --verbose add \
          ${PACKAGES} \
          ${BUILD_PACKAGES} \
          ${LIB_PACKAGES} \
    && \
    mkdir -p ${MYDUMPER_BUILD_PATH} && \
    tar --strip-components=1 -xzvf /tmp/mydumper.tar.gz -C ${MYDUMPER_BUILD_PATH} && \
    rm /tmp/mydumper.tar.gz && \
    cd ${MYDUMPER_BUILD_PATH} && \
    cmake . "${MYDUMPER_CMAKE_ARGS}" && \
    make && \
    mv ./mydumper /usr/local/bin/. && \
    mv ./myloader /usr/local/bin/. && \
    cd / && rm -rf ${MYDUMPER_BUILD_PATH} \
    && \
    # Clean up
    apk del ${BUILD_PACKAGES} && \
    rm -f /usr/lib/*.a && \
    (rm "/tmp/"* 2>/dev/null || true) && \
    (rm -rf /var/cache/apk/* 2>/dev/null || true)

WORKDIR /
RUN curl -Lo /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
    && chmod +x /bin/dumb-init \
    && dumb-init -V

COPY --from=builder /pkg/ /usr/
COPY static /static/
COPY k8s_templates /k8s_templates/
ENTRYPOINT ["dumb-init", "--"]
CMD ["/usr/bin/backup"]
