FROM ruby:3.3.5-alpine3.20

LABEL org.opencontainers.image.authors="Andrew Kane <andrew@ankane.org>"

RUN apk add --update build-base libpq-dev && \
    gem install pgslice && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["pgslice"]
