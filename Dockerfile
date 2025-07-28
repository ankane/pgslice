FROM ruby:3-alpine

LABEL org.opencontainers.image.authors="Andrew Kane <andrew@ankane.org>"

RUN apk add --update gcompat libpq-dev && \
    gem install pgslice && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["pgslice"]
