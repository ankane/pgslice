require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

ENV["PGSLICE_ENV"] = "test"
