SET client_min_messages = warning;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

CREATE TABLE "Users" (
  "Id" SERIAL PRIMARY KEY
);

CREATE TABLE "Posts" (
  "Id" SERIAL PRIMARY KEY,
  "UserId" INTEGER,
  "createdAt" timestamp,
  "createdAtTz" timestamptz,
  "createdOn" date,
  CONSTRAINT "foreign_key_1" FOREIGN KEY ("UserId") REFERENCES "Users"("Id")
);

CREATE INDEX ON "Posts" ("createdAt");

INSERT INTO "Posts" ("createdAt", "createdAtTz", "createdOn") SELECT NOW(), NOW(), NOW() FROM generate_series(1, 10000) n;
