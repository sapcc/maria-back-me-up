FROM keppel.eu-de-1.cloud.sap/ccloud-dockerhub-mirror/library/golang:1.24-alpine as builder
WORKDIR /go/src/github.com/sapcc/maria-back-me-up
RUN apk add --no-cache make git
COPY . /src
RUN make -C /src install PREFIX=/pkg GO_BUILDFLAGS='-mod vendor'

FROM keppel.eu-de-1.cloud.sap/ccloud-dockerhub-mirror/library/alpine:3.22
LABEL maintainer="Stefan Hipfel <stefan.hipfel@sap.com>"
LABEL source_repository="https://github.com/sapcc/maria-back-me-up"

ENV PACKAGES="mysql-client mariadb curl" \
    LIB_PACKAGES="glib-dev mariadb-dev zlib-dev pcre-dev libressl-dev" \
    BUILD_PACKAGES="cmake build-base git" \
    BUILD_PATH="/opt/mydumper-src/"

RUN apk --no-cache add \
    $PACKAGES \
    $BUILD_PACKAGES \
    $LIB_PACKAGES \
    && \
    git clone https://github.com/maxbube/mydumper.git $BUILD_PATH && \
    cd $BUILD_PATH && \
    git checkout 2cdd78599ea7e13f36c6e8a09051814f6bd1564a && \
    cmake . && \
    make && \
    mv ./mydumper /usr/bin/. && \
    mv ./myloader /usr/bin/. && \
    cd / && rm -rf $BUILD_PATH && \
    apk del $BUILD_PACKAGES && \
    rm -f /usr/lib/*.a && \
    (rm "/tmp/"* 2>/dev/null || true) && \
    (rm -rf /var/cache/apk/* 2>/dev/null || true)

WORKDIR /
RUN curl -Lo /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_amd64 \
    && chmod +x /bin/dumb-init \
    && dumb-init -V

COPY --from=builder /pkg/ /usr/
COPY static /static/
COPY k8s_templates /k8s_templates/
ENTRYPOINT ["dumb-init", "--"]
CMD ["/usr/bin/backup"]
