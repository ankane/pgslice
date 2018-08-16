module PgSlice
  class Table
    attr_reader :table

    def initialize(table)
      @table = table
    end

    def to_s
      table
    end

    def intermediate_table
      self.class.new("#{table}_intermediate")
    end

    def retired_table
      self.class.new("#{table}_retired")
    end

    def exists?
      existing_tables(like: table).any?
    end

    # http://www.dbforums.com/showthread.php?1667561-How-to-list-sequences-and-the-columns-by-SQL
    def sequences
      query = <<-SQL
        SELECT
          a.attname as related_column,
          s.relname as sequence_name
        FROM pg_class s
          JOIN pg_depend d ON d.objid = s.oid
          JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid
          JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum)
          JOIN pg_namespace n ON n.oid = s.relnamespace
        WHERE s.relkind = 'S'
          AND n.nspname = $1
          AND t.relname = $2
      SQL
      execute(query, table.split(".", 2))
    end

    private

    def existing_tables(like:)
      query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename LIKE $2"
      execute(query, like.split(".", 2)).map { |r| "#{r["schemaname"]}.#{r["tablename"]}" }.sort
    end

    def execute(*args)
      $client.send(:execute, *args)
    end
  end
end
