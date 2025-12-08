module PgSlice
  class CLI
    desc "enable_retired_mirroring TABLE", "Enable mirroring triggers from TABLE to TABLE_retired"
    def enable_retired_mirroring(table_name)
      table = create_table(table_name)
      retired_table = table.retired_table

      assert_table(table)
      assert_table(retired_table)

      enable_retired_mirroring_triggers(table)
      log("Retired mirroring triggers enabled for #{table_name}")
    end
  end
end
