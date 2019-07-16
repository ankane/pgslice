module PgSlice
  class CLI < Thor
    class << self
      attr_accessor :instance
    end

    include Helpers

    check_unknown_options!

    class_option :url, desc: "Database URL"
    class_option :dry_run, type: :boolean, default: false, desc: "Print statements without executing"

    map %w[--version -v] => :version

    def self.exit_on_failure?
      ENV["PGSLICE_ENV"] != "test"
    end

    def initialize(*args)
      PgSlice::CLI.instance = self
      $stdout.sync = true
      $stderr.sync = true
      super
    end

    desc "version", "Show version"
    def version
      log("pgslice #{PgSlice::VERSION}")
    end
  end
end
