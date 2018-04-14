require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

$conn = PG::Connection.open(dbname: "pgslice_test")
$conn.exec <<-SQL
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS "Foo" CASCADE;
CREATE SCHEMA "Foo";
CREATE TABLE "Foo"."Users" (
  "Id" SERIAL PRIMARY KEY
);
CREATE TABLE "Foo"."Posts" (
  "Id" SERIAL PRIMARY KEY,
  "UserId" INTEGER,
  "createdAt" timestamp,
  CONSTRAINT "foreign_key_1" FOREIGN KEY ("UserId") REFERENCES "Foo"."Users"("Id")
);
CREATE INDEX ON "Foo"."Posts" ("createdAt");
INSERT INTO "Foo"."Posts" ("createdAt") SELECT NOW() FROM generate_series(1, 10000) n;
SQL
