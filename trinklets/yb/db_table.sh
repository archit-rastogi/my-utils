#!/bin/bash

# Fetch all database names
export PGPASSWORD='Yugabyte!1'
databases=$(ysqlsh -U yugabyte -h 172.165.29.124,172.165.39.118,172.165.56.95 -t -c "SELECT datname FROM pg_database WHERE datistemplate = false;")

for db in $databases; do
  echo "$db |" `ysqlsh -U yugabyte -h 172.165.29.124,172.165.39.118,172.165.56.95  -d $db -t -c \
    "SELECT count(table_name)
     FROM information_schema.tables
     WHERE table_type = 'BASE TABLE'
       AND table_schema NOT IN ('pg_catalog', 'information_schema');"`
done
