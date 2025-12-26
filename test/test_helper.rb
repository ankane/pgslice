require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"

PgSlice::CLI.exit_on_failure = false
