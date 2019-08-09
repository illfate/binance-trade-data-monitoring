FROM golang:1.12.6 as build
ADD . /binance-trade-data-monitoring
WORKDIR /binance-trade-data-monitoring
RUN make

FROM alpine
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
COPY --from=build /binance-trade-data-monitoring/bin/btdm .
CMD ["./btdm"]
