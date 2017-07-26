require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

def conn
  @conn ||= PG::Connection.open(dbname: "pgslice_test")
  @conn.exec "SET client_min_messages = warning"
  @conn
end

def create_tables
  drop_tables
  conn.exec <<-SQL
  CREATE TABLE "Users" (
    "Id" SERIAL PRIMARY KEY
  );
  CREATE TABLE "Posts" (
    "Id" SERIAL PRIMARY KEY,
    "UserId" INTEGER,
    "createdAt" timestamp,
    CONSTRAINT "foreign_key_1" FOREIGN KEY ("UserId") REFERENCES "Users"("Id")
  );
  CREATE INDEX ON "Posts" ("createdAt");
  SQL
end

def populate_data
  conn.exec <<-SQL
  INSERT INTO "Posts" (SELECT n AS id, NULL, NOW() FROM generate_series(1, 10000) n);
  SQL
end

def drop_tables
  conn.exec <<-SQL
  SET client_min_messages = warning;
  DROP TABLE IF EXISTS "Posts_intermediate" CASCADE;
  DROP TABLE IF EXISTS "Posts" CASCADE;
  DROP TABLE IF EXISTS "Posts_retired" CASCADE;
  DROP FUNCTION IF EXISTS "Posts_insert_trigger"();
  DROP TABLE IF EXISTS "Users" CASCADE;
  SQL
end
