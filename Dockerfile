FROM bitnami/minideb:jessie
COPY ./bundles/kubeless_linux-amd64/kafka-controller /kafka-controller
ENTRYPOINT ["/kafka-controller"]