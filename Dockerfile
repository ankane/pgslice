FROM ruby:latest
RUN gem install pgslice
ENTRYPOINT ["pgslice"]
