FROM keppel.eu-de-1.cloud.sap/ccloud-dockerhub-mirror/library/golang:1.25-bookworm as builder
WORKDIR /go/src/github.com/sapcc/maria-back-me-up
RUN apt-get update && apt-get install -y make git
COPY . /src
RUN make -C /src install PREFIX=/pkg GO_BUILDFLAGS='-mod vendor'

FROM keppel.eu-de-1.cloud.sap/ccloud-dockerhub-mirror/library/mariadb:10.11

LABEL maintainer="Stefan Hipfel <stefan.hipfel@sap.com>"
LABEL source_repository="https://github.com/sapcc/maria-back-me-up"

RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-client \
        ca-certificates \
        bash \
        curl \
        gzip \
        mydumper \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /
RUN curl -Lo /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64 \
    && chmod +x /bin/dumb-init \
    && dumb-init -V

COPY --from=builder /pkg/ /usr/
COPY static /static/
COPY k8s_templates /k8s_templates/
ENTRYPOINT ["dumb-init", "--"]
CMD ["/usr/bin/backup"]
