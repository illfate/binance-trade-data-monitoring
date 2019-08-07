FROM golang:1.12.6 as build
ADD . /binance-trade-data-monitoring
WORKDIR /binance-trade-data-monitoring
RUN make

FROM scratch
COPY --from=build /binance-trade-data-monitoring/bin/btdm .
CMD ["./btdm"]
