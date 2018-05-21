FROM ruby:2.5.1-alpine3.7

MAINTAINER Andrew Kane <andrew@chartkick.com>

RUN apk add --update ruby-dev build-base \
  libxml2-dev libxslt-dev pcre-dev libffi-dev \
  postgresql-dev

RUN gem install pgslice

ENTRYPOINT ["pgslice"]
