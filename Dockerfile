# STEP 1 build executable binary

FROM golang:alpine as builder
RUN go version
COPY . $GOPATH/src/github.com/kubeless/kafka-trigger/
RUN ls -lah $GOPATH/src/github.com/kubeless/kafka-trigger/
WORKDIR $GOPATH/src/github.com/kubeless/kafka-trigger/cmd/kafka-trigger-controller

# get dependancies and build the executable:
# RUN go get -d -v
RUN go build -o /kafka-controller


# STEP 2 build a small image

FROM bitnami/minideb:jessie
COPY --from=builder /kafka-controller /kafka-controller
ENTRYPOINT ["/kafka-controller"]