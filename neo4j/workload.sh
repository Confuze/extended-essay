#!/bin/bash

sleep 10

./bin/ycsb load neo4j -P workloads/workloada -P db.properties -p recordcount=100000 -s > load-neo4j.dat
cp load-neo4j.dat /data

./bin/ycsb run neo4j -P workloads/workloada -P db.properties -p operationcount=100000 -s > transaction-neo4j.dat
cp transaction-neo4j.dat /data
