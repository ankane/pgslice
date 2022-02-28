FROM ruby:3.1.1-alpine3.15

MAINTAINER Andrew Kane <andrew@chartkick.com>

RUN apk add --update ruby-dev build-base \
  libxml2-dev libxslt-dev pcre-dev libffi-dev \
  postgresql-dev

RUN gem install pgslice

ENTRYPOINT ["pgslice"]
