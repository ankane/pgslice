require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

conn = PG::Connection.open(dbname: "pgslice_test")
conn.exec <<-SQL
SET client_min_messages = warning;
DROP TABLE IF EXISTS posts_intermediate CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS posts_retired CASCADE;
DROP FUNCTION IF EXISTS posts_insert_trigger();
CREATE TABLE posts (
  id integer PRIMARY KEY,
  created_at timestamp
);
INSERT INTO posts (SELECT n AS id, NOW() FROM generate_series(1, 10000) n);
SQL
