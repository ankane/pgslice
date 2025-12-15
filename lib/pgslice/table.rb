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
      query = <<~SQL
        SELECT COUNT(*) FROM pg_catalog.pg_tables
        WHERE schemaname = $1 AND tablename = $2
      SQL
      execute(query, [schema, name]).first["count"].to_i > 0
    end

    def columns
      query = <<~SQL
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND is_generated = 'NEVER'
      SQL
      execute(query, [schema, name]).map { |r| r["column_name"] }
    end

    # http://www.dbforums.com/showthread.php?1667561-How-to-list-sequences-and-the-columns-by-SQL
    def sequences
      query = <<~SQL
        SELECT
          a.attname AS related_column,
          n.nspname AS sequence_schema,
          s.relname AS sequence_name
        FROM pg_class s
          INNER JOIN pg_depend d ON d.objid = s.oid
          INNER JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid
          INNER JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum)
          INNER JOIN pg_namespace n ON n.oid = s.relnamespace
          INNER JOIN pg_namespace nt ON nt.oid = t.relnamespace
        WHERE s.relkind = 'S'
          AND nt.nspname = $1
          AND t.relname = $2
        ORDER BY s.relname ASC
      SQL
      execute(query, [schema, name])
    end

    def foreign_keys
      query = <<~SQL
        SELECT pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid = $1::regclass AND contype ='f'
      SQL
      execute(query, [quote_table]).map { |r| r["pg_get_constraintdef"] }
    end

    # https://stackoverflow.com/a/20537829
    # TODO can simplify with array_position in Postgres 9.5+
    def primary_key
      query = <<~SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod),
          pg_attribute.attnum,
          pg_index.indkey
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
      rows = execute(query, [schema, name])
      rows.sort_by { |r| r["indkey"].split(" ").index(r["attnum"]) }.map { |r| r["attname"] }
    end

    def index_defs
      query = <<~SQL
        SELECT pg_get_indexdef(indexrelid) FROM pg_index
        WHERE indrelid = $1::regclass AND indisprimary = 'f'
      SQL
      execute(query, [quote_table]).map { |r| r["pg_get_indexdef"] }
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
      query = <<~SQL
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
      SQL
      data_type = execute(query, [schema, name, column])[0]["data_type"]
      data_type == "timestamp with time zone" ? "timestamptz" : "date"
    end

    def max_id(primary_key, below: nil, where: nil)
      query = "SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_table}"
      conditions = []
      params = []
      if below
        conditions << "#{quote_ident(primary_key)} <= $1"
        params << below
      end
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      result = execute(query, params)[0]["max"]
      
      # For ULIDs, return as string (or nil if empty); for numeric, convert to int (0 if nil)
      if result.nil?
        # Check if we're dealing with ULIDs by sampling a row
        sample_query = "SELECT #{quote_ident(primary_key)} FROM #{quote_table} LIMIT 1"
        sample_result = execute(sample_query)[0]
        if sample_result && sample_result[primary_key]
          handler = id_handler(sample_result[primary_key])
          return handler.is_a?(Helpers::UlidHandler) ? nil : 0
        else
          return 0  # Default to numeric (0) when no sample available
        end
      end
      
      handler = id_handler(result)
      handler.is_a?(Helpers::NumericHandler) ? result.to_i : result
    end

    def min_id(primary_key, column, cast, starting_time, where)
      query = "SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_table}"
      conditions = []
      conditions << "#{quote_ident(column)} >= #{sql_date(starting_time, cast)}" if starting_time
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      result = execute(query)[0]["min"]
      
      # Return appropriate default and type based on primary key type
      if result.nil?
        # Check if we're dealing with ULIDs by sampling a row
        sample_query = "SELECT #{quote_ident(primary_key)} FROM #{quote_table} LIMIT 1"
        sample_result = execute(sample_query)[0]
        if sample_result
          handler = id_handler(sample_result[primary_key])
          return handler.min_value
        else
          return 1  # Default numeric when no sample available
        end
      end
      
      # Return the actual result with proper type
      handler = id_handler(result)
      handler.is_a?(Helpers::NumericHandler) ? result.to_i : result
    end

    # ensure this returns partitions in the correct order
    def partitions
      query = <<~SQL
        SELECT
          nmsp_child.nspname AS schema,
          child.relname AS name
        FROM pg_inherits
          JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
          JOIN pg_class child ON pg_inherits.inhrelid = child.oid
          JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
          JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
        WHERE
          nmsp_parent.nspname = $1 AND
          parent.relname = $2
        ORDER BY child.relname ASC
      SQL
      execute(query, [schema, name]).map { |r| Table.new(r["schema"], r["name"]) }
    end

    def fetch_comment
      execute("SELECT obj_description($1::regclass) AS comment", [quote_table])[0]
    end

    def fetch_trigger(trigger_name)
      query = <<~SQL
        SELECT obj_description(oid, 'pg_trigger') AS comment FROM pg_trigger
        WHERE tgname = $1 AND tgrelid = $2::regclass
      SQL
      execute(query, [trigger_name, quote_table])[0]
    end

    def fetch_settings(trigger_name)
      needs_comment = false
      trigger_comment = fetch_trigger(trigger_name)
      comment = trigger_comment || fetch_comment
      if comment
        field, period, cast, version = comment["comment"].split(",").map { |v| v.split(":").last } rescue []
        version = version.to_i if version
      end

      unless period
        needs_comment = true
        function_def = execute("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = $1", [trigger_name])[0]
        return [] unless function_def
        function_def = function_def["pg_get_functiondef"]
        sql_format = Helpers::SQL_FORMAT.find { |_, f| function_def.include?("'#{f}'") }
        return [] unless sql_format
        period = sql_format[0]
        field = /to_char\(NEW\.(\w+),/.match(function_def)[1]
      end

      # backwards compatibility with 0.2.3 and earlier (pre-timestamptz support)
      unless cast
        cast = "date"
        # update comment to explicitly define cast
        needs_comment = true
      end

      unless ["date", "timestamptz"].include?(cast)
        abort "Invalid cast"
      end

      version ||= trigger_comment ? 1 : 2
      declarative = version > 1

      [period, field, cast, needs_comment, declarative, version]
    end

    protected

    def abort(message)
      PgSlice::CLI.instance.send(:abort, message)
    end

    def execute(*args)
      PgSlice::CLI.instance.send(:execute, *args)
    end

    def quote(value)
      PgSlice::CLI.instance.send(:quote, value)
    end

    def quote_ident(value)
      PgSlice::CLI.instance.send(:quote_ident, value)
    end

    def sql_date(*args)
      PgSlice::CLI.instance.send(:sql_date, *args)
    end

    def id_handler(sample_id)
      PgSlice::CLI.instance.send(:id_handler, sample_id)
    end
  end
end
