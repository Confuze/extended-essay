#!/usr/bin/env bash
set -euo pipefail

COMMAND="$1"
DBMS="$2"
WORKLOAD="${3-}"
DEPTH=${4:-1}

WL_LOWER=$(echo "$WORKLOAD" | tr 'A-Z' 'a-z')

BASE_DIR="/workload"
JAR_PATH="$BASE_DIR/app.jar"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"

OUT_DIR="/results/${DBMS}/workload-${WORKLOAD}"

NODES_PATH="$BASE_DIR/dataset/nodes.txt"
EDGES_PATH="$BASE_DIR/dataset/edges.txt"

OPERATIONS=100000
THREADS=8

case "$COMMAND" in
	load)
		exec java -jar "${JAR_PATH}" load "${DBMS}" "${NODES_PATH}" "${EDGES_PATH}"
		;;
	run)
		if [[ -z "${WORKLOAD}" ]]; then
      echo "Missing workload name for 'run' command." >&2
      echo "Usage: $0 run <dbms> <workload>" >&2
      exit 1
    fi

    mkdir -p "${OUT_DIR}"

    echo "Running WARMUP for DBMS=${DBMS}, workload=${WORKLOAD}"
    echo "  depth=${DEPTH}, ops=${OPERATIONS}, threads=${THREADS}"
		
    # warmup: discard output
    java -jar "${JAR_PATH}" run "${DBMS}" "${WORKLOAD}" \
      "${DEPTH}" "${OPERATIONS}" "${THREADS}"

    for i in 1 2 3; do
      OUT_FILE="${OUT_DIR}/run-${i}-${TIMESTAMP}.txt"
      echo "Running MEASURED run ${i} -> ${OUT_FILE}"
      java -jar "${JAR_PATH}" run "${DBMS}" "${WORKLOAD}" \
        "${DEPTH}" "${OPERATIONS}" "${THREADS}" "${OUT_FILE}"
    done
		;;
		*)
			echo "Unknown command '${COMMAND}'. Expected 'load' or 'run'." >&2
			exit 1
		;;
esac
