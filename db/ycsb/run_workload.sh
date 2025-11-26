#!/usr/bin/env bash
set -euo pipefail

DBMS="${1}"
WORKLOAD="${2}"

WL_LOWER=$(echo "$WORKLOAD" | tr 'A-Z' 'a-z')

BASE_DIR="/workload"
COMMON_PROPS="${BASE_DIR}/conf/common.properties"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"

OUT_DIR="/results/${DBMS}/workload${WORKLOAD}"
mkdir -p "$OUT_DIR"


run_ycsb() {
  local dbms="$1"
  local binding="$2"
  local workload="$3"
  local common_props="$4"
  local db_props="$5"
  local out_dir="$6"
  shift 6
  local extra_args=( "$@" )   # remaining args = DBMS-specific flags

  local ycsb_bin="$BASE_DIR/bin/ycsb"
  local workload_file="workloads/workload${workload}"

  echo "Running YCSB for DBMS=${dbms}, workload=${workload}, binding=${binding}"
  echo "Output dir: ${out_dir}"

  "${ycsb_bin}" load "${binding}" \
    -s \
    -P "${workload_file}" \
    -P "${common_props}" \
    -P "${db_props}" \
    -p exportfile="${out_dir}/load-${dbms}-${TIMESTAMP}.txt" \
    "${extra_args[@]}"

  # warmup run
  "${ycsb_bin}" run "${binding}" \
    -s \
    -P "${workload_file}" \
    -P "${common_props}" \
    -P "${db_props}" \
    -p exportfile="${out_dir}/warmup-${dbms}-${TIMESTAMP}.txt" \
    "${extra_args[@]}"

  for i in 1 2 3; do
    "${ycsb_bin}" run "${binding}" \
      -s \
      -P "${workload_file}" \
      -P "${common_props}" \
      -P "${db_props}" \
      -p exportfile="${out_dir}/run-${i}-${dbms}-${TIMESTAMP}.txt" \
      "${extra_args[@]}"
  done
}

case "$DBMS" in
  postgres)
    BINDING="jdbc"
    DB_PROPS="${BASE_DIR}/conf/postgres.properties"
    run_ycsb \
      "postgres" \
      "jdbc" \
      "${WL_LOWER}" \
      "${COMMON_PROPS}" \
      "${DB_PROPS}" \
      "${OUT_DIR}" \
      -cp "${BASE_DIR}/postgresql-connector-java.jar"
    ;;

  neo4j)
    BINDING="neo4j"
    DB_PROPS="${BASE_DIR}/conf/neo4j.properties"
    run_ycsb \
      "neo4j" \
      "neo4j" \
      "${WL_LOWER}" \
      "${COMMON_PROPS}" \
      "${DB_PROPS}" \
      "${OUT_DIR}" \
    ;;

      *)
    echo "Unsupported DBMS '${DBMS}'. Expected 'postgres' or 'neo4j'." >&2
    exit 1
    ;;
esac

