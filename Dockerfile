FROM hub.byted.org/alarm/golang:1.13.5 as builder
RUN go get -u github.com/golang/dep/...
WORKDIR /go/src/github.com/influxdata/influxdb
#COPY Gopkg.toml Gopkg.lock ./
#RUN dep ensure -vendor-only
COPY . /go/src/github.com/influxdata/influxdb
RUN export GO111MODULE=off;go install ./cmd/...

FROM hub.byted.org/alarm/golang:1.13.5
COPY --from=builder /go/bin/* /usr/bin/
COPY --from=builder /go/src/github.com/influxdata/influxdb/etc/config.sample.toml /etc/influxdb/influxdb.conf

EXPOSE 8086
VOLUME /var/lib/influxdb

COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/init-influxdb.sh /init-influxdb.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["influxd"]
