require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

PgSlice::CLI.exit_on_failure = false
