FROM ruby:3.1.1-alpine3.15

MAINTAINER Andrew Kane <andrew@chartkick.com>

RUN apk add --update libpq ruby-dev build-base libxml2-dev libxslt-dev pcre-dev libffi-dev postgresql-dev && \
    gem install pgslice && \
    apk del ruby-dev build-base libxml2-dev libxslt-dev pcre-dev libffi-dev postgresql-dev && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["pgslice"]
