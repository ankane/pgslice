require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

conn = PG::Connection.open(dbname: "pgslice_test")
conn.exec <<-SQL
SET client_min_messages = warning;
DROP TABLE IF EXISTS "Posts_intermediate" CASCADE;
DROP TABLE IF EXISTS "Posts" CASCADE;
DROP TABLE IF EXISTS "Posts_retired" CASCADE;
DROP FUNCTION IF EXISTS "Posts_insert_trigger"();
CREATE TABLE "Posts" (
  "Id" SERIAL PRIMARY KEY,
  "createdAt" timestamp
);
CREATE INDEX ON "Posts" ("createdAt");
INSERT INTO "Posts" (SELECT n AS id, NOW() FROM generate_series(1, 10000) n);
SQL
