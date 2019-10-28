FROM golang:1.12.0-alpine3.9 as builder
WORKDIR /go/src/github.com/sapcc/maria-back-me-up
RUN apk add --no-cache make
COPY . .
ARG VERSION
RUN make all

FROM alpine:3.9
LABEL maintainer="Stefan Hipfel <stefan.hipfel@sap.com>"

RUN apk update && apk add mysql-client
RUN apk add --no-cache curl
RUN curl -Lo /bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
	&& chmod +x /bin/dumb-init \
	&& dumb-init -V
COPY --from=builder /go/src/github.com/sapcc/maria-back-me-up/bin/linux/maria-back-me-up /usr/local/bin/
COPY templates /templates/
ENTRYPOINT ["dumb-init", "--"]
CMD ["maria-back-me-up"]