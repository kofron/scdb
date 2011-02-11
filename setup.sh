#!/usr/bin/env bash
createdb -h localhost scdb
psql -h localhost -d scdb < create_support.sql
psql -h localhost -d scdb < create_tables.sql
psql -h localhost -d scdb < create_func_trig.sql