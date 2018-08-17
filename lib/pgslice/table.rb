module PgSlice
  class Table < GenericTable
    def intermediate_table
      self.class.new("#{table}_intermediate")
    end

    def retired_table
      self.class.new("#{table}_retired")
    end

    def trigger_name
      "#{table.split(".")[-1]}_insert_trigger"
    end

    def column_cast(column)
      data_type = execute("SELECT data_type FROM information_schema.columns WHERE table_schema || '.' || table_name = $1 AND column_name = $2", [table, column])[0]["data_type"]
      data_type == "timestamp with time zone" ? "timestamptz" : "date"
    end

    def max_id(primary_key, below: nil, where: nil)
      query = "SELECT MAX(#{quote_ident(primary_key)}) FROM #{quote_table(table)}"
      conditions = []
      conditions << "#{quote_ident(primary_key)} <= #{below}" if below
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      execute(query)[0]["max"].to_i
    end

    def min_id(primary_key, column, cast, starting_time, where)
      query = "SELECT MIN(#{quote_ident(primary_key)}) FROM #{quote_table(table)}"
      conditions = []
      conditions << "#{quote_ident(column)} >= #{sql_date(starting_time, cast)}" if starting_time
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      (execute(query)[0]["min"] || 1).to_i
    end

    def existing_partitions(period = nil)
      count =
        case period
        when "day"
          8
        when "month"
          6
        when "year"
          4
        else
          "6,8"
        end

      existing_tables(like: "#{table}_%").select { |t| /\A#{Regexp.escape("#{table}_")}\d{#{count}}\z/.match(t) }
    end

    def fetch_comment
      execute("SELECT obj_description(#{regclass(table)}) AS comment")[0]
    end

    def fetch_trigger(trigger_name)
      execute("SELECT obj_description(oid, 'pg_trigger') AS comment FROM pg_trigger WHERE tgname = $1 AND tgrelid = #{regclass(table)}", [trigger_name])[0]
    end

    protected

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
