module PgSlice
  class CLI < Thor
    class << self
      attr_accessor :instance, :exit_on_failure
      alias_method :exit_on_failure?, :exit_on_failure
    end
    self.exit_on_failure = true

    include Helpers

    check_unknown_options!

    class_option :url, desc: "Database URL"
    class_option :dry_run, type: :boolean, default: false, desc: "Print statements without executing"

    map %w[--version -v] => :version

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

    desc "enable_mirroring TABLE", "Enable mirroring triggers for live data changes during partitioning"
    def enable_mirroring(table_name)
      table = create_table(table_name)
      intermediate_table = table.intermediate_table
      
      assert_table(table)
      assert_table(intermediate_table)
      
      enable_mirroring_triggers(table)
      log("Mirroring triggers enabled for #{table_name}")
    end

    desc "disable_mirroring TABLE", "Disable mirroring triggers after partitioning is complete"
    def disable_mirroring(table_name)
      table = create_table(table_name)
      
      assert_table(table)
      
      disable_mirroring_triggers(table)
      log("Mirroring triggers disabled for #{table_name}")
    end
  end
end
