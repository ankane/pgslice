module PgSlice
  class CLI
    desc "prep TABLE [COLUMN] [PERIOD]", "Create an intermediate table for partitioning"
    option :partition, type: :boolean, default: true, desc: "Partition the table"
    option :trigger_based, type: :boolean, default: false, desc: "Use trigger-based partitioning"
    def prep(table, column=nil, period=nil)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      trigger_name = table.trigger_name

      unless options[:partition]
        abort "Usage: \"pgslice prep TABLE --no-partition\"" if column || period
        abort "Can't use --trigger-based and --no-partition" if options[:trigger_based]
      end
      assert_table(table)
      assert_no_table(intermediate_table)

      if options[:partition]
        abort "Usage: \"pgslice prep TABLE COLUMN PERIOD\"" if !(column && period)
        abort "Column not found: #{column}" unless table.columns.include?(column)
        abort "Invalid period: #{period}" unless SQL_FORMAT[period.to_sym]
      end

      queries = []

      version =
        if options[:trigger_based] || server_version_num < 100000
          1
        elsif server_version_num < 110000
          2
        else
          3
        end

      declarative = version > 1

      if declarative && options[:partition]
        queries << <<-SQL
CREATE TABLE #{quote_table(intermediate_table)} (LIKE #{quote_table(table)} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE (#{quote_ident(column)});
        SQL

        if version == 3
          index_defs = table.index_defs
          index_defs.each do |index_def|
            queries << make_index_def(index_def, intermediate_table)
          end

          table.foreign_keys.each do |fk_def|
            queries << make_fk_def(fk_def, intermediate_table)
          end
        end

        # add comment
        cast = table.column_cast(column)
        queries << <<-SQL
COMMENT ON TABLE #{quote_table(intermediate_table)} is 'column:#{column},period:#{period},cast:#{cast},version:#{version}';
        SQL
      else
        queries << <<-SQL
CREATE TABLE #{quote_table(intermediate_table)} (LIKE #{quote_table(table)} INCLUDING ALL);
        SQL

        table.foreign_keys.each do |fk_def|
          queries << make_fk_def(fk_def, intermediate_table)
        end
      end

      if options[:partition] && !declarative
        queries << <<-SQL
CREATE FUNCTION #{quote_ident(trigger_name)}()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'Create partitions first.';
    END;
    $$ LANGUAGE plpgsql;
        SQL

        queries << <<-SQL
CREATE TRIGGER #{quote_ident(trigger_name)}
    BEFORE INSERT ON #{quote_table(intermediate_table)}
    FOR EACH ROW EXECUTE PROCEDURE #{quote_ident(trigger_name)}();
        SQL

        cast = table.column_cast(column)
        queries << <<-SQL
COMMENT ON TRIGGER #{quote_ident(trigger_name)} ON #{quote_table(intermediate_table)} is 'column:#{column},period:#{period},cast:#{cast}';
        SQL
      end

      run_queries(queries)
    end
  end
end
