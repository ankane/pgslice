module PgSlice
  class Table
    attr_reader :schema, :name

    def initialize(schema, name)
      @schema = schema
      @name = name
    end

    def to_s
      [schema, name].join(".")
    end

    def exists?
      execute("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2", [schema, name]).first["count"].to_i > 0
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

    def intermediate_table
      self.class.new(schema, "#{name}_intermediate")
    end

    def retired_table
      self.class.new(schema, "#{name}_retired")
    end

    def trigger_name
      "#{name}_insert_trigger"
    end

    def column_cast(column)
      data_type = execute("SELECT data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3", [schema, name, column])[0]["data_type"]
      data_type == "timestamp with time zone" ? "timestamptz" : "date"
    end

    def max_id(primary_key, below: nil, where: nil)
      query = "SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_table}"
      conditions = []
      conditions << "#{quote_ident(primary_key)} <= #{below}" if below
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      execute(query)[0]["max"].to_i
    end

    def min_id(primary_key, column, cast, starting_time, where)
      query = "SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_table}"
      conditions = []
      conditions << "#{quote_ident(column)} >= #{sql_date(starting_time, cast)}" if starting_time
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      (execute(query)[0]["min"] || 1).to_i
    end

    def partitions
      query = <<-SQL
        SELECT
          nmsp_child.nspname  AS schema,
          child.relname       AS name
        FROM pg_inherits
          JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
          JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
          JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
          JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
        WHERE
          nmsp_parent.nspname = $1 AND
          parent.relname = $2
      SQL
      execute(query, [schema, name]).map { |r| Table.new(r["schema"], r["name"]) }
    end

    def fetch_comment
      execute("SELECT obj_description(#{regclass}) AS comment")[0]
    end

    def fetch_trigger(trigger_name)
      execute("SELECT obj_description(oid, 'pg_trigger') AS comment FROM pg_trigger WHERE tgname = $1 AND tgrelid = #{regclass}", [trigger_name])[0]
    end

    protected

    def execute(*args)
      PgSlice::CLI.instance.send(:execute, *args)
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def regclass
      "'#{quote_table}'::regclass"
    end

    def sql_date(time, cast, add_cast = true)
      if cast == "timestamptz"
        fmt = "%Y-%m-%d %H:%M:%S UTC"
      else
        fmt = "%Y-%m-%d"
      end
      str = "'#{time.strftime(fmt)}'"
      add_cast ? "#{str}::#{cast}" : str
    end
  end
end
