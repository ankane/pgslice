module PgSlice
  class GenericTable
    attr_reader :schema, :name

    def initialize(schema, name)
      @schema = schema
      @name = name
    end

    def to_s
      [schema, name].join(".")
    end

    def exists?
      existing_tables(schema, like: name).any?
    end

    def columns
      execute("SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", [schema, name]).map{ |r| r["column_name"] }
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
      execute(query, [schema, name])
    end

    def foreign_keys
      execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = #{regclass} AND contype ='f'").map { |r| r["pg_get_constraintdef"] }
    end

    # http://stackoverflow.com/a/20537829
    def primary_key
      query = <<-SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          nspname = $1 AND
          relname = $2 AND
          indrelid = pg_class.oid AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      SQL
      execute(query, [schema, name]).map { |r| r["attname"] }
    end

    def index_defs
      execute("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = #{regclass} AND indisprimary = 'f'").map { |r| r["pg_get_indexdef"] }
    end

    def quote_table
      [quote_ident(schema), quote_ident(name)].join(".")
    end

    protected

    def existing_tables(schema, like:)
      query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename LIKE $2 ORDER BY 1, 2"
      execute(query, [schema, like]).map { |r| Table.new(r["schemaname"], r["tablename"]) }
    end

    def execute(*args)
      $client.send(:execute, *args)
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def regclass
      "'#{quote_table}'::regclass"
    end
  end
end
