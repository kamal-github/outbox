FROM golang:1.23 AS builder

ARG VERSION=dev
ARG USER=outbox

WORKDIR /code
COPY . .
RUN go build -o outbox /code/cmd

FROM ubuntu:bionic

RUN groupadd -r outbox && useradd --no-log-init -r -g outbox outbox
USER outbox

COPY --from=builder --chown=outbox:outbox /code/outbox /bin/outbox

ENTRYPOINT ["outbox"]
