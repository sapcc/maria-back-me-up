FROM golang:1.12.0-alpine3.9 as builder
WORKDIR /go/src/github.com/sapcc/maria-back-me-up
RUN apk add --no-cache make
COPY . .
ARG VERSION
RUN make all

FROM alpine:3.9
LABEL maintainer="Stefan Hipfel <stefan.hipfel@sap.com>"

ENV PACKAGES="mysql-client curl" \
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
    cmake . && \
    make && \
    mv ./mydumper /usr/bin/. && \
    mv ./myloader /usr/bin/. && \
    cd / && rm -rf $BUILD_PATH && \
    apk del $BUILD_PACKAGES && \
    rm -f /usr/lib/*.a && \
    (rm "/tmp/"* 2>/dev/null || true) && \
    (rm -rf /var/cache/apk/* 2>/dev/null || true)

RUN curl -Lo /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
	&& chmod +x /bin/dumb-init \
	&& dumb-init -V
COPY --from=builder /go/src/github.com/sapcc/maria-back-me-up/bin/linux/maria-back-me-up /usr/local/bin/
COPY templates /templates/
ENTRYPOINT ["dumb-init", "--"]
CMD ["maria-back-me-up"]