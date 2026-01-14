#!/usr/bin/env bash
set -euo pipefail

# =====:: global config

# Default workers
readonly WORKERS=(
    "worker1"
    "worker2"
    "worker3"
    "worker4"
)

# =====:: guide

usage() {
    cat << EOF

==================================================
Spark Cluster Management Tool
==================================================

Usage: $0 [OPTIONS]

[OPTIONS]:
    -h, --help                          :Show help
    -l, --location  [system|local]      :Target install location. Default: local (~/.local)
    -t, --type      <hadoop|spark|all>  :Select component type
    -w, --workers   [list]              :Update 'workers' file in config dirs
    -c, --config    [list]              :Sync config dirs from master to workers
    -s, --sync-all  [list]              :Sync EVERYTHING (Java, Hadoop, Spark) to workers
    -r, --run       <script-path>       :Run a benchmark script (e.g., spark-submit script)

Examples:
    # Update workers and sync config to all (local)
    $0 -t all -w worker1 worker2 -c worker1 worker2

    # Sync all binaries to a new worker (system location)
    $0 -l system -s worker5

    # Run a benchmark script
    $0 -r ./run_benchmark.sh

==================================================
EOF
}

# =====:: utilities

# Determine base directory based on location
get_base_dir() {
    local loc="$1"
    if [[ "$loc" == "system" ]]; then
        echo "/opt"
    else
        echo "$HOME/.local/opt"
    fi
}

check_ssh() {
    local worker="$1"
    ssh -o BatchMode=yes -o ConnectTimeout=5 "$USER@$worker" "echo" >/dev/null 2>&1
}

update_workers() {
    local type="$1"
    local loc="$2"
    shift 2
    local target_workers=("$@")

    local base_dir=$(get_base_dir "$loc")
    local files=()
    [[ "$type" == "hadoop" || "$type" == "all" ]] && files+=("$base_dir/Hadoop/current/etc/hadoop/workers")
    [[ "$type" == "spark"  || "$type" == "all" ]] && files+=("$base_dir/Spark/current/conf/workers")

    for f in "${files[@]}"; do
        if [ ! -d "$(dirname "$f")" ]; then
            echo "[WARN] Directory $(dirname "$f") does not exist. Skipping."
            continue
        fi
        printf "%s\n" "${target_workers[@]}" > "$f"
        echo "[INFO] Updated $f"
    done
}

send_config() {
    local type="$1"
    local loc="$2"
    shift 2
    local target_workers=("$@")

    local base_dir=$(get_base_dir "$loc")
    local dirs=()
    [[ "$type" == "hadoop" || "$type" == "all" ]] && dirs+=("$base_dir/Hadoop/current/etc/hadoop")
    [[ "$type" == "spark"  || "$type" == "all" ]] && dirs+=("$base_dir/Spark/current/conf")

    for worker in "${target_workers[@]}"; do
        if check_ssh "$worker"; then
            echo "==> Syncing $type config to $worker ($loc)..."
            for d in "${dirs[@]}"; do
                if [ -d "$d" ]; then
                    rsync -avz --delete "$d/" "$USER@$worker:$d/"
                else
                    echo "[WARN] Source directory $d does not exist. Skipping."
                fi
            done
        else
            echo "[WARN] Worker $worker unreachable, skipping."
        fi
    done
}

sync_all() {
    local loc="$1"
    shift
    local target_workers=("$@")

    local base_dir=$(get_base_dir "$loc")
    local dirs=("$base_dir/Java" "$base_dir/Hadoop" "$base_dir/Spark")

    for worker in "${target_workers[@]}"; do
        if check_ssh "$worker"; then
            echo "==> Syncing ALL binaries to $worker ($loc)..."
            # Ensure remote dir exists
            if [[ "$loc" == "system" ]]; then
                ssh "$USER@$worker" "sudo mkdir -p $base_dir"
            else
                ssh "$USER@$worker" "mkdir -p $base_dir"
            fi

            for d in "${dirs[@]}"; do
                if [ -d "$d" ]; then
                    echo "[INFO] Syncing $d..."
                    if [[ "$loc" == "system" ]]; then
                        rsync -avz --rsync-path="sudo rsync" "$d/" "$USER@$worker:$d/"
                    else
                        rsync -avz "$d/" "$USER@$worker:$d/"
                    fi
                fi
            done
            # Sync bashrc for environment variables
            rsync -avz "$HOME/.bashrc" "$USER@$worker:$HOME/.bashrc"
            echo "[INFO] Sync to $worker completed."
        else
            echo "[WARN] Worker $worker unreachable, skipping."
        fi
    done
}

run_script() {
    local path="$1"
    if [ ! -f "$path" ]; then
        echo "[ERROR] Script not found: $path"
        return 1
    fi
    chmod +x "$path"
    echo "[INFO] Running $path..."
    "$path"
    echo "[INFO] Finished running $path."
}

# =====:: main

main() {
    local LOCATION="local"
    local TYPE=""
    local WORKERS_LIST=("${WORKERS[@]}")
    local RUN_PATH=""
    local SYNC_ALL_TARGETS=()
    local CONFIG_TARGETS=()
    local UPDATE_WORKERS=false
    local SEND_CONFIG=false
    local DO_SYNC_ALL=false

    [[ $# -eq 0 ]] && { usage; exit 0; }

    while [[ $# -gt 0 ]]; do
        case "$1" in
            '-h'|'--help')      usage; exit 0 ;;
            '-l'|'--location')  LOCATION="$2"; shift 2 ;;
            '-t'|'--type')      TYPE="$2"; shift 2 ;;
            '-w'|'--workers')
                UPDATE_WORKERS=true
                shift
                WORKERS_LIST=()
                while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do WORKERS_LIST+=("$1"); shift; done
                ;;
            '-c'|'--config')
                SEND_CONFIG=true
                shift
                CONFIG_TARGETS=()
                while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do CONFIG_TARGETS+=("$1"); shift; done
                ;;
            '-s'|'--sync-all')
                DO_SYNC_ALL=true
                shift
                SYNC_ALL_TARGETS=()
                while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do SYNC_ALL_TARGETS+=("$1"); shift; done
                ;;
            '-r'|'--run')
                RUN_PATH="$2"; shift 2
                ;;
            *) echo "[ERROR] Unknown option: $1"; usage; exit 1 ;;
        esac
    done

    # Execute actions
    if $UPDATE_WORKERS; then
        if [[ -z "$TYPE" ]]; then echo "[ERROR] -t <hadoop|spark|all> is required for -w"; exit 1; fi
        update_workers "$TYPE" "$LOCATION" "${WORKERS_LIST[@]}"
    fi

    if $SEND_CONFIG; then
        if [[ -z "$TYPE" ]]; then echo "[ERROR] -t <hadoop|spark|all> is required for -c"; exit 1; fi
        [[ ${#CONFIG_TARGETS[@]} -eq 0 ]] && CONFIG_TARGETS=("${WORKERS_LIST[@]}")
        send_config "$TYPE" "$LOCATION" "${CONFIG_TARGETS[@]}"
    fi

    if $DO_SYNC_ALL; then
        [[ ${#SYNC_ALL_TARGETS[@]} -eq 0 ]] && SYNC_ALL_TARGETS=("${WORKERS_LIST[@]}")
        sync_all "$LOCATION" "${SYNC_ALL_TARGETS[@]}"
    fi

    if [[ -n "$RUN_PATH" ]]; then
        run_script "$RUN_PATH"
    fi
}

main "$@"
