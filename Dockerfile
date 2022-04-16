########################################################
# STEP 1 use a temporary image to build a static binary
########################################################
FROM golang:1.18  AS builder

# Pull build dependencies
ENV GOPATH=$APP_ROOT
ENV GOBIN=$APP_ROOT/bin
USER root


WORKDIR $GOPATH/src//
COPY go.mod go.sum ./

RUN go mod download
COPY . .

# Build static image.

RUN go get -d github.com/alvaroloes/enumer && \
 go generate ./... && \ 
 GIT_SHA=$(git rev-parse --short HEAD) && \
 CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -a \
 -ldflags "-extldflags '-static' -w -s -X main.appSha=$GIT_SHA" \
 -o /opt/app-root/src/github.com/ignalina/fixed2arrow/fixed2arrow-monolith \
 github.com/ignalina/fixed2arrow


FROM alpine:3.15
USER root

RUN mkdir /app \
  && chown -R 1001:1001 /app


USER 1001
WORKDIR /opt/bin
COPY --from=builder /opt/app-root/src/github.com/ignalina/fixed2arrow/fixed2arrow-monolith /opt/bin/fixed2arrow-monolith

CMD /opt/bin/fixed2arrow-monolith
