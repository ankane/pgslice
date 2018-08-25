module PgSlice
  class CLI
    desc "analyze TABLE", "Analyze tables"
    option :swapped, type: :boolean, default: false, desc: "Use swapped table"
    def analyze(table)
      table = create_table(table)
      parent_table = options[:swapped] ? table : table.intermediate_table

      period, _, _, _, _ = fetch_settings(table, parent_table)
      analyze_list = table.partitions(period) + [parent_table]
      run_queries_without_transaction(analyze_list.map { |t| "ANALYZE VERBOSE #{quote_table(t)};" })
    end
  end
end
