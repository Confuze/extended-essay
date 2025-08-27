#!/bin/sh

./bin/ycsb load jdbc -P workloads/workloada -P db.properties -p recordcount=100000 -s -cp postgresql-connector-java.jar > load-postgresql.dat
cp load-postgresql.dat /data

./bin/ycsb run jdbc -P workloads/workloada -P db.properties -p operationcount=100000 -s -cp postgresql-connector-java.jar > transaction-postgresql.dat
cp transaction-postgresql.dat /data
