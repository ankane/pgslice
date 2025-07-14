module PgSlice
  class CLI
    desc "unprep TABLE", "Undo prep"
    def unprep(table)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      trigger_name = table.trigger_name

      assert_table(intermediate_table)

      queries = [
        "DROP TABLE #{quote_table(intermediate_table)} CASCADE;"
      ]

      _, _, _, _, declarative, _ = intermediate_table.fetch_settings(table.trigger_name)
      unless declarative
        queries << "DROP FUNCTION IF EXISTS #{quote_ident(trigger_name)}();"
      end

      run_queries(queries)
    end
  end
end
