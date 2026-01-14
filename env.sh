#!/usr/bin/env bash
set -euo pipefail

# =====:: global config

# Base directory: ~/.local
readonly SYSTEM_DIR="/opt"
readonly LOCAL_DIR="$HOME/.local"

BASE_DIR="$LOCAL_DIR"
BIN_DIR="$BASE_DIR/bin"
OPT_DIR="$BASE_DIR/opt"
DATA_DIR="$BASE_DIR/data"
CACHE_DIR="$BASE_DIR/cache"

HADOOP_NAMENODE_DIR="$DATA_DIR/hadoop/hdfs/namenode"
HADOOP_DATANODE_DIR="$DATA_DIR/hadoop/hdfs/datanode"
HADOOP_TMP_DIR="$DATA_DIR/hadoop/tmp"
SPARK_LOCAL_DIR="$DATA_DIR/spark/local"

JAVA_ROOT="$OPT_DIR/Java"
PYTHON_ROOT="$OPT_DIR/Python"
SCALA_ROOT="$OPT_DIR/Scala"
HADOOP_ROOT="$OPT_DIR/Hadoop"
SPARK_ROOT="$OPT_DIR/Spark"

readonly HADOOP_LOG_DIR="hdfs:///hadoop/eventlog"
readonly SPARK_LOG_DIR="hdfs:///spark/eventlog"
readonly SPARK_CHECKPOINT_DIR="hdfs:///spark/checkpoint"

# Shell config and install location targets
RC_TARGET="auto"        # auto | bashrc | profile
LOCATION_TARGET="local" # local | system
OFFLINE_MODE=false
ONLY_DOWNLOAD=false

# =====:: Cluster Configuration
# Adjust these parameters based on your cluster setup

# Cluster topology
readonly MASTER_HOST="master"
readonly WORKER_NODES=4          # Number of worker nodes

# Master node resources (2 CPU / 4 GB)
readonly MASTER_VCORES=2
readonly MASTER_MEMORY_MB=4096

# Worker node resources (6 CPU / 12 GB per node)
readonly WORKER_VCORES=6
readonly WORKER_MEMORY_MB=12288

# HDFS configuration
readonly HDFS_REPLICATION=2                    # Replication factor (min 2 for fault tolerance)
readonly HDFS_BLOCK_SIZE_MB=128                # Block size in MB (128MB default)

# YARN NodeManager configuration (per worker node)
# Leave ~1GB for OS and other services
readonly YARN_NM_MEMORY_MB=11264
readonly YARN_NM_VCORES=6                      # All 6 cores available

# YARN container limits
readonly YARN_CONTAINER_MIN_MEMORY_MB=512
readonly YARN_CONTAINER_MAX_MEMORY_MB=10240
readonly YARN_CONTAINER_MIN_VCORES=1           # Minimum container vcores
readonly YARN_CONTAINER_MAX_VCORES=6

# Spark Standalone configuration
readonly SPARK_MASTER_HOST="$MASTER_HOST"
readonly SPARK_MASTER_PORT=7077
readonly SPARK_MASTER_WEBUI_PORT=8080

# Spark Worker configuration (per worker node)
# 2 executors per node, each executor gets 2 cores
readonly SPARK_WORKER_CORES=6                  # Total cores per worker
readonly SPARK_WORKER_MEMORY="11g"
readonly SPARK_WORKER_INSTANCES=1              # One worker daemon per physical node

# Spark Executor configuration (for Standalone mode)
# Total: 4 workers × 2 executors = 8 executors
readonly SPARK_EXECUTOR_CORES=2                # 2 cores per executor
readonly SPARK_EXECUTOR_MEMORY="5g"
readonly SPARK_EXECUTOR_INSTANCES=2            # Executors per worker node

# Spark Driver configuration
readonly SPARK_DRIVER_MEMORY="3g"
readonly SPARK_DRIVER_CORES=2                  # Driver cores

# Spark performance tuning
readonly SPARK_DEFAULT_PARALLELISM=48
readonly SPARK_SQL_SHUFFLE_PARTITIONS=64
readonly SPARK_MEMORY_FRACTION=0.7

# Spark YARN mode configuration
readonly SPARK_YARN_EXECUTOR_CORES=2           # Same as standalone
readonly SPARK_YARN_EXECUTOR_MEMORY="5g"
readonly SPARK_YARN_EXECUTOR_INSTANCES=8       # 4 workers × 2 executors = 8 total
readonly SPARK_YARN_DRIVER_MEMORY="3g"
readonly SPARK_YARN_DRIVER_CORES=2             # Driver cores
readonly SPARK_YARN_AM_MEMORY="2g"

# =====:: guide

usage() {
    cat << EOF

# ================================================== #

Usage: $0 [OPTIONS]

[OPTIONS]:
    -h --help                       :Show help (Default)
    -j --java   #                   :Versions: 17 ...
    -p --python #.#                 :Versions: 3.11 ...
    -P --pypy   #.#                 :Versions: 3.11 ...
    -s --scala  #.#.#               :Versions: 2.12.18 ...
    -H --hadoop #.#.#               :Versions: 3.3.6 ...
    -S --spark  #.#.#               :Versions: 3.5.7 ...
    -l --location [system|local]    :Target install location. Default: local (~/.local)
    --rc [bashrc|profile]           :Target shell config file. Default: auto
    --offline                       :Offline mode (use cache, skip download)
    --only-download                 :Only download to cache, skip installation

# ================================================== #

EOF
}

# =====:: utilities


smart_download() {
    local url="$1"
    local dest="$2"
    local filename=$(basename "$dest")
    local cache_file="$CACHE_DIR/$filename"

    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$CACHE_DIR"
        sudo chown "$USER:$USER" "$CACHE_DIR" 2>/dev/null || true
    else
        mkdir -p "$CACHE_DIR"
    fi

    if [[ -f "$cache_file" ]]; then
        printf "[INFO] Using cached file: %s
" "$cache_file"
        cp -f "$cache_file" "$dest"
        return 0
    fi

    if [[ "$OFFLINE_MODE" == "true" ]]; then
        printf "[ERROR] Offline mode enabled but file not in cache: %s
" "$filename"
        exit 1
    fi

    printf "[INFO] Downloading: %s
" "$url"

    local download_ok=0
    if command -v aria2c >/dev/null 2>&1; then
        aria2c -x 16 -s 16 -j 16 -k 1M --min-split-size=1M --max-connection-per-server=16 --split=16 --file-allocation=none --console-log-level=error --summary-interval=0 -d "$(dirname "$dest")" -o "$filename" "$url" && download_ok=1
    else
        curl -fL --progress-bar -o "$dest" "$url" && download_ok=1
    fi

    if [[ $download_ok -eq 1 ]]; then
        cp -f "$dest" "$cache_file"
        return 0
    else
        return 1
    fi
}


require_sudo() {
    sudo -v || {
        echo "[ERROR] sudo required"
        exit 1
    }
}

detect_os() {
    [[ -f /etc/os-release ]] || { echo "unknown"; return 1; }
    . /etc/os-release || { echo "unknown"; return 1; }
    case "$ID" in
        'ubuntu'|'linuxmint'|'kali')    echo "debian" ;;
        'fedora')                       echo "fedora" ;;
        'arch')                         echo "arch" ;;
        *)                              echo "other" ;;
    esac
}

ensure_cmd() {
    local mgr="$1"; shift
    local missing=()

    for cmd in "$@"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done
    [[ ${#missing[@]} -eq 0 ]] && return 0

    printf "\n[INFO] Installing: %s\n" "${missing[*]}"

    case "$mgr" in
        apt)
            sudo apt update -y || {
                printf "[ERROR] apt update failed.\n"
                return 1
            }
            sudo apt install -y "${missing[@]}" || {
                printf "[ERROR] Failed to install: %s\n" "${missing[*]}"
                return 1
            }
            ;;
        dnf)
            sudo dnf install -y "${missing[@]}" || {
                printf "[ERROR] Failed to install: %s\n" "${missing[*]}"
                return 1
            }
            ;;
        pacman)
            sudo pacman -Syu --noconfirm || {
                printf "[ERROR] pacman system upgrade failed.\n"
                return 1
            }
            sudo pacman -S --noconfirm "${missing[@]}" || {
                printf "[ERROR] Failed to install: %s\n" "${missing[*]}"
                return 1
            }
            ;;
        *)
            printf "\n[WARN] Unsupported package manager.\n\n"
            return 1
            ;;
    esac
    printf "\n[INFO] Packages installed successfully.\n\n"
    return 0
}

install_dependencies() {
    require_sudo
    case "$(detect_os)" in
        'debian')
            ensure_cmd apt wget curl aria2 git make build-essential jq unzip \
                gnome-tweaks micro \
                openssh-server openssh-client rsync \
                libssl-dev zlib1g-dev libbz2-dev libreadline-dev \
                libsqlite3-dev libncurses5-dev libncursesw5-dev \
                libffi-dev liblzma-dev tk-dev xz-utils \
                libxml2-dev libxmlsec1-dev llvm
            ;;
        'fedora')
            ensure_cmd dnf wget curl aria2 git make gcc gcc-c++ gdb jq unzip tar gzip bzip2 xz \
                gnome-tweaks micro \
                openssh-server openssh-clients rsync \
                openssl-devel zlib-devel bzip2-devel readline-devel \
                sqlite-devel ncurses-devel libffi-devel xz-devel \
                tk-devel libxml2-devel xmlsec1-devel llvm
            ;;
        'arch')
            ensure_cmd pacman wget curl aria2 git tar gzip bzip2 unzip xz jq \
                gnome-tweaks micro \
                openssh rsync \
                base-devel openssl zlib libffi
            ;;
        *)
            echo "[WARN] OS not supported."
            return 1
            ;;
    esac
    echo "[INFO] All required dependencies are installed and up-to-date!"
    echo
}

ensure_bin() {
    local rc="$(resolve_rc)"
    [[ -z "$rc" ]] && rc="$HOME/.profile"
    local begin="# >>> bin path (managed)"
    local end="# <<< bin path"
    local bin_path="$BIN_DIR"

    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$bin_path"
        sudo chown -R "$USER:$USER" "$bin_path"
        echo "[INFO] Created system bin directory: $bin_path"
    else
        mkdir -p "$bin_path"
        echo "[INFO] Created local bin directory: $bin_path"
    fi
    touch "$rc"

    if grep -Fq "$begin" "$rc" 2>/dev/null; then
        local current_path=$(sed -n "/$begin/,/$end/p" "$rc" | grep -oP 'export PATH="\K[^:]+')
        if [[ "$current_path" == "$bin_path" ]]; then
            printf "[INFO] $bin_path already managed in [$rc]\n\n"
            return 0
        else
            echo "[INFO] Updating bin path from [$current_path] to [$bin_path]"
            if [[ ! -w "$rc" ]]; then sudo sed -i "\|$begin|,\|$end|d" "$rc" 2>/dev/null || true; else sed -i "\|$begin|,\|$end|d" "$rc" 2>/dev/null || true; fi
        fi
    fi

    if grep -q "PATH=.*$bin_path" "$rc" 2>/dev/null; then
        printf "[INFO] $bin_path already exists in PATH (unmanaged)\n"
        printf "[INFO] Adding managed block for consistency\n"
    fi

    local line="export PATH=\"$bin_path:\$PATH\""
    local bin_block=$(printf "\n%s\n\n%s\n\n%s\n" "$begin" "$line" "$end")
    if [[ ! -w "$rc" ]]; then
        echo "$bin_block" | sudo tee -a "$rc" > /dev/null
        sudo chown "$USER:$USER" "$rc"
    else
        echo "$bin_block" >> "$rc"
    fi

    printf "[INFO] $bin_path added to PATH in [$rc]\n"
    printf "[INFO] Run 'source %s' to apply changes\n\n" "$rc"
}

ensure_managed_block() {
    local rc="$1"
    local begin="$2"
    local end="$3"
    shift 3
    [ "$#" -gt 0 ] || return 0

    local block_content
    block_content=$(cat <<EOF
$begin
$(printf '%s\n' "$@")
$end
EOF
)

    if [[ ! -w "$rc" ]]; then
        sudo sed -i "\|$begin|,\|$end|d" "$rc" 2>/dev/null || true
        printf '%s\n' "$block_content" | sudo tee -a "$rc" > /dev/null
        sudo chown "$USER:$USER" "$rc"
    else
        sed -i "\|$begin|,\|$end|d" "$rc" 2>/dev/null || true
        printf '%s\n' "$block_content" >> "$rc"
    fi
}

resolve_rc() {
    case "$RC_TARGET" in
        bashrc)  echo "$HOME/.bashrc" ;;
        profile) echo "$HOME/.profile" ;;
        auto)    echo "" ;;
        *)
            echo "[ERROR] Invalid --rc value: $RC_TARGET"
            exit 1
            ;;
    esac
}

apply_location_target() {
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        require_sudo
        BASE_DIR="$SYSTEM_DIR"
        BIN_DIR="$BASE_DIR/bin"
        OPT_DIR="$BASE_DIR"
        DATA_DIR="$BASE_DIR/data"
        CACHE_DIR="$BASE_DIR/cache"

        HADOOP_NAMENODE_DIR="$DATA_DIR/hadoop/hdfs/namenode"
        HADOOP_DATANODE_DIR="$DATA_DIR/hadoop/hdfs/datanode"
        HADOOP_TMP_DIR="$DATA_DIR/hadoop/tmp"
        SPARK_LOCAL_DIR="$DATA_DIR/spark/local"

        JAVA_ROOT="$OPT_DIR/Java"
        PYTHON_ROOT="$OPT_DIR/Python"
        SCALA_ROOT="$OPT_DIR/Scala"
        HADOOP_ROOT="$OPT_DIR/Hadoop"
        SPARK_ROOT="$OPT_DIR/Spark"
    fi
}

# =====:: java

print_java_guide() {
    cat << 'EOF'

# ================================================== #

Usage:

1. Reload shell:

    source ~/.bashrc

2. Check Java version:

    java -version

3. Change Java version:

    use-java [1]      # Show Usage

    use-java jdk-17   # Switch Version

# ================================================== #

EOF
}

setup_java() {
    # check version
    local version="$1"
    local supported=(8 11 16 17 18 19 20 21 22 23 24 25)
    [[ -n "$version" ]] || {
        echo "[ERROR] Missing Java version"
        echo "Usage: $0 <${supported[*]}>"
        return 1
    }
    [[ " ${supported[*]} " =~ " $version " ]] || {
        echo "[ERROR] Unsupported Java version: $version"
        return 1
    }

    # get link
    printf "\n===== Installing Java %s =====\n" "$version"
    local api="https://api.github.com/repos/adoptium/temurin${version}-binaries/releases/latest"
    local api_json="$(curl -fsSL -H "User-Agent: Mozilla/5.0" "$api" 2>&1 )"
    local curl_exit=$?

    local url=""
    local v=""
    local ver_main=""
    local ver_build=""
    if [[ $curl_exit -ne 0 ]] || ! echo "$api_json" | jq empty 2>/dev/null; then
        echo "[WARN] GitHub API unavailable or rate limited, using direct URL..."
        case "$version" in
            8)
                url="https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u472-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u472b08.tar.gz"
                ;;
            11 )
                v="11.0.29+7"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin11-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK11U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            16 )
                v="16.0.2+7"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin16-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK16U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            17 )
                v="17.0.17+10"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK17U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            18 )
                v="18.0.2.1+1"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin18-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK18U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            19 )
                v="19.0.2+7"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin19-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK19U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            20 )
                v="20.0.2+9"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin20-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK20U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            21 )
                v="21.0.9+10"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK21U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            22 )
                v="22.0.2+9"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin22-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK22U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            23 )
                v="23.0.2+7"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin23-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK23U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            24 )
                v="24.0.2+12"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin24-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK24U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            25 )
                v="25.0.1+8"
                ver_main="${v%+*}"
                ver_build="${v#*+}"
                url="https://github.com/adoptium/temurin25-binaries/releases/download/jdk-${ver_main}%2B${ver_build}/OpenJDK25U-jdk_x64_linux_hotspot_${ver_main}_${ver_build}.tar.gz"
                ;;
            * )
                echo "[ERROR] No fallback URL available for Java $version"
                echo "[INFO] Please try again later or check GitHub releases manually"
                return 1
                ;;
        esac
    else
        url="$(
            printf '%s' "$api_json" |
            jq -r '
                .assets[]
                | select(.browser_download_url
                    | test("OpenJDK.*-jdk_x64_linux_hotspot.*\\.tar\\.gz$"))
                | .browser_download_url
            ' |
            head -n1
        )"
        [[ -n "$url" ]] || {
            echo "[ERROR] Failed to fetch download URL from API"
            return 1
        }
    fi

    # download
    local tmp_dir; tmp_dir="$(mktemp -d)"; trap "rm -rf \'$tmp_dir\'" RETURN INT TERM
    local name="jdk-$version"
    local archive="$tmp_dir/${name}.tar.gz"
    smart_download "$url" "$archive" || {
        echo "[ERROR] Download failed"
        return 1
    }
    [[ "$ONLY_DOWNLOAD" == "true" ]] && { printf "[INFO] Only download mode: skipped installation for %s\n" "$name"; return 0; }

    # extract
    tar -xzf "$archive" -C "$tmp_dir"
    local extracted="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    [[ -n "$extracted" ]] || {
        echo "[ERROR] Extraction failed"
        return 1
    }

    # move
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$JAVA_ROOT"
        sudo chown -R "$USER:$USER" "$JAVA_ROOT"
    else
        mkdir -p "$JAVA_ROOT"
    fi
    local setup_dir="$JAVA_ROOT/$name"
    rm -rf "$setup_dir"
    mv "$extracted" "$setup_dir"

    # symlink
    ln -sfn "$setup_dir" "$JAVA_ROOT/current"

    # done
    printf "\n[INFO] Java %s installed and set as current\n\n" "$version"
    "$JAVA_ROOT/current/bin/java" -version 2>&1 | head -n1
}

install_java_tool() {
        if [[ "$LOCATION_TARGET" == "system" ]]; then
        cat << EOF | sudo tee "$BIN_DIR/use-java" > /dev/null
#!/usr/bin/env bash

ROOT="$JAVA_ROOT"

EOF
        cat << 'EOF' | sudo tee -a "$BIN_DIR/use-java" > /dev/null
usage() {
    printf "\nUsage: use-java <jdk-#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Java root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Java version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"

"$ROOT/current/bin/java" -version
EOF
    else
        cat << EOF > "$BIN_DIR/use-java"
#!/usr/bin/env bash

ROOT="$JAVA_ROOT"

EOF
        cat << 'EOF' >> "$BIN_DIR/use-java"
usage() {
    printf "\nUsage: use-java <jdk-#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Java root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Java version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"

"$ROOT/current/bin/java" -version
EOF
    fi

    if [[ "$LOCATION_TARGET" == "system" ]]; then sudo chmod +x "$BIN_DIR/use-java"; else chmod +x "$BIN_DIR/use-java"; fi
}

# =====:: python
# =====:: pypy

print_python_guide() {
    cat << 'EOF'

# ================================================== #

Usage:

1. Reload shell:

    source ~/.bashrc

2. Check Python version:

    python --version

3. Change Python version:

    use-python [1]               # Show Usage

    use-python python-3.11    # Switch Version

    use-python pypy-3.11         # Switch Version

# ================================================== #

EOF
}

get_lastest_python_version() {
    local mm="$1"

    # validate input
    if [ -z "$mm" ]; then
        echo "[ERROR] Usage: resolve_python_version <major.minor>" >&2
        return 1
    fi
    if ! echo "$mm" | grep -qE '^[0-9]+\.[0-9]+$'; then
        echo "[ERROR] Invalid format. Expected: <major.minor> (e.g., 3.11)" >&2
        return 1
    fi

    # check python ftp
    local versions
    versions=$(
        curl -fsSL "https://www.python.org/ftp/python/" 2>/dev/null |
        grep -oE "href=\"${mm}\.[^/]+/\"" |
        sed -E 's/.*href="([^/"]+)\/".*/\1/' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$'
    )
    if [ -n "$versions" ]; then
        local preview=$(echo "$versions" | grep -E '[abrc]' | sort -V | tail -1)
        if [ -n "$preview" ]; then
            echo "$preview"
            return 0
        fi
        local stable=$(echo "$versions" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)
        if [ -n "$stable" ]; then
            echo "$stable"
            return 0
        fi
    fi

    # check windows json indexes
    local json_urls=(
        "https://www.python.org/ftp/python/index-windows.json"
        "https://www.python.org/ftp/python/index-windows-recent.json"
        "https://www.python.org/ftp/python/index-windows-legacy.json"
     )
    for url in "${json_urls[@]}"; do
        versions=$(
            curl -fsSL "$url" 2>/dev/null |
            jq -r --arg mm "$mm" '
                .versions[]
                    | select(.company == "PythonCore")
                    | select(.id | test("^pythoncore-" + $mm + "-"))
                | .["sort-version"]
            ' 2>/dev/null
        )
        if [ -n "$versions" ]; then
            local preview=$(echo "$versions" | grep -E '[abrc]' | sort -V | tail -1)
            if [ -n "$preview" ]; then
                echo "$preview"
                return 0
            fi
            local stable=$(echo "$versions" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)
            if [ -n "$stable" ]; then
                echo "$stable"
                return 0
            fi
        fi
    done

    # not found
    echo "[ERROR] Python $mm not found" >&2
    return 1
}

setup_python() {
    # check version
    local version="$1"
    [[ -n "$version" ]] || {
        echo "[ERROR] Missing Python version"
        return 1
    }
    if ! echo "$version" | grep -qE '^[0-9]+\.[0-9]+$'; then
        echo "[ERROR] Invalid format. Expected: <major.minor> (e.g., 3.11)" >&2
        return 1
    fi
    local full_version="$(get_lastest_python_version "$version")"
    [[ -n "$full_version" ]] || {
        echo "[ERROR] Cannot resolve Python version"
        return 1
    }
    version="${full_version%.*}"

    # download
    printf "\n===== Installing CPython %s =====\n" "$full_version"
    local tmp_dir; tmp_dir="$(mktemp -d)"; trap "rm -rf \'$tmp_dir\'" RETURN INT TERM
    local name="python-${version}"
    local archive="$tmp_dir/${name}.tgz"
    local url="https://www.python.org/ftp/python/${full_version}/Python-${full_version}.tgz"
    smart_download "$url" "$archive" || {
        echo "[ERROR] Download failed"
        return 1
    }
    [[ "$ONLY_DOWNLOAD" == "true" ]] && { printf "[INFO] Only download mode: skipped installation for %s\n" "$name"; return 0; }

    # extract
    tar -xzf "$archive" -C "$tmp_dir"
    local extracted="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1 )"
    [[ -n "$extracted" ]] || {
        echo "[ERROR] Extraction failed"
        return 1
    }

    # move
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$PYTHON_ROOT"
        sudo chown -R "$USER:$USER" "$PYTHON_ROOT"
    else
        mkdir -p "$PYTHON_ROOT"
    fi
    local setup_dir="$PYTHON_ROOT/$name"
    rm -rf "$setup_dir"
    mv "$extracted" "$setup_dir"

    # build
    echo ">> Build CPython: $name"
    cd "$setup_dir" || {
        echo "[ERROR] Cannot enter build directory"
        return 1
    }
    ./configure --prefix="$setup_dir" --enable-optimizations --with-ensurepip=install > /dev/null 2>&1 || {
        echo "[ERROR] Configure failed"
        return 1
    }
    make -j"$(nproc)" > /dev/null 2>&1 || {
        echo "[ERROR] Build failed"
        return 1
    }
    make install > /dev/null 2>&1 || {
        echo "[ERROR] Install failed"
        return 1
    }

    # shorcut
    if [[ -x "$setup_dir/bin/python${version}" && ! -x "$setup_dir/bin/python" ]]; then
        ln -sfn "$setup_dir/bin/python${version}" "$setup_dir/bin/python"
    fi

    # pip
    "$setup_dir/bin/python" -m pip install --upgrade pip setuptools wheel virtualenv

    # symlink
    ln -sfn "$setup_dir" "$PYTHON_ROOT/current"

    # done
    printf "\n[INFO] Python %s installed and set as current\n\n" "$version"
    "$PYTHON_ROOT/current/bin/python" --version 2>&1 | head -n1
}

get_lastest_pypy_version() {
    local py_version="$1"
    local base_url="https://downloads.python.org/pypy"

    # validate input
    if [ -z "$py_version" ]; then
        echo "[ERROR] Usage: find_latest_pypy_version <py_version>" >&2
        return 1
    fi
    if ! echo "$py_version" | grep -qE '^[0-9]+\.[0-9]+$'; then
        echo "[ERROR] Invalid format. Expected: <major.minor> (e.g., 3.11)" >&2
        return 1
    fi

    # find all pypy versions
    local pypy_versions
    pypy_versions=$(
        curl -fsSL "${base_url}/" 2>/dev/null |
        grep -oE "pypy${py_version}-v[0-9]+\.[0-9]+\.[0-9]+(-[a-z0-9]+)?-linux64\.tar\.bz2" |
        sed -E 's/.*-v([0-9]+\.[0-9]+\.[0-9]+(-[a-z0-9]+)?)-.*/\1/' |
        sort -uV
    )
    if [ -n "$pypy_versions" ]; then
        local stable=$(echo "$pypy_versions" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | tail -1)
        if [ -n "$stable" ]; then
            echo "$stable"
            return 0
        fi
        local latest=$(echo "$pypy_versions" | tail -1)
        if [ -n "$latest" ]; then
            echo "$latest"
            return 0
        fi
    fi

    # Linear search for the latest version (7.3.x)
    local major=7
    local minor=3
    local found_version=""
    local url status
    for patch in {0..99}; do
        url="${base_url}/pypy${py_version}-v${major}.${minor}.${patch}-linux64.tar.bz2"
        status=$(curl -fsSL -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
        if [ "$status" = "200" ]; then
            found_version="${major}.${minor}.${patch}"
        else
            break
        fi
    done
    if [ -n "$found_version" ]; then
        echo "$found_version"
        return 0
    fi
    local known_versions=("7.3.22" "7.3.21" "7.3.20" "7.3.19" "7.3.18" "7.3.17" "7.3.16" "7.3.15" "7.3.14" "7.3.13" "7.3.12" "7.3.11" "7.3.10")
    for version in "${known_versions[@]}"; do
        url="${base_url}/pypy${py_version}-v${version}-linux64.tar.bz2"
        status=$(curl -fsSL -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
        if [ "$status" = "200" ]; then
            echo "$version"
            return 0
        fi
    done

    # Not found
    echo "[ERROR] No PyPy version found for Python ${py_version}" >&2
    return 1
}

setup_pypy() {
    # check version
    local py_version="$1"
    [[ -n "$py_version" ]] || {
        echo "[ERROR] Missing PyPy version"
        return 1
    }
    if ! echo "$py_version" | grep -qE '^[0-9]+\.[0-9]+$'; then
        echo "[ERROR] Invalid format. Expected: <major.minor> (e.g., 3.11)" >&2
        return 1
    fi
    local pypy_version="$(get_lastest_pypy_version "$py_version")"
    if [ $? -ne 0 ]; then
        echo "[ERROR] Cannot find PyPy version for Python ${py_version}" >&2
        return 1
    fi

    # download
    printf "\n===== Installing PyPy %s (v%s) =====\n" "$py_version" "$pypy_version"
    local tmp_dir; tmp_dir="$(mktemp -d)"; trap "rm -rf \'$tmp_dir\'" RETURN INT TERM
    local name="pypy-${py_version}"
    local archive="$tmp_dir/${name}.tar.bz2"
    local url="https://downloads.python.org/pypy/pypy${py_version}-v${pypy_version}-linux64.tar.bz2"
    smart_download "$url" "$archive" || {
        echo "[ERROR] Download failed"
        return 1
    }
    [[ "$ONLY_DOWNLOAD" == "true" ]] && { printf "[INFO] Only download mode: skipped installation for %s\n" "$name"; return 0; }

    # extract
    tar -xjf "$archive" -C "$tmp_dir"
    local extracted="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    [[ -n "$extracted" ]] || {
        echo "[ERROR] Extraction failed"
        return 1
    }

    # move
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$PYTHON_ROOT"
        sudo chown -R "$USER:$USER" "$PYTHON_ROOT"
    else
        mkdir -p "$PYTHON_ROOT"
    fi
    local setup_dir="$PYTHON_ROOT/$name"
    rm -rf "$setup_dir"
    mv "$extracted" "$setup_dir"

    # shorcut
    if [[ -x "$setup_dir/bin/pypy${py_version}" && ! -x "$setup_dir/bin/python" ]]; then
        ln -sfn "$setup_dir/bin/pypy${py_version}" "$setup_dir/bin/python"
    fi

    # pip
    "$setup_dir/bin/python" -m ensurepip --upgrade
    "$setup_dir/bin/python" -m pip install --upgrade pip setuptools wheel virtualenv

    # symlink
    ln -sfn "$setup_dir" "$PYTHON_ROOT/current"

    # done
    printf "\n[INFO] PyPy %s (v%s) installed and set as current\n\n" "$py_version" "$pypy_version"
    "$PYTHON_ROOT/current/bin/python" --version 2>&1 | head -n1
}

install_python_tool() {
        if [[ "$LOCATION_TARGET" == "system" ]]; then
        cat << EOF | sudo tee "$BIN_DIR/use-python" > /dev/null
#!/usr/bin/env bash

ROOT="$PYTHON_ROOT"

EOF
        cat << 'EOF' | sudo tee -a "$BIN_DIR/use-python" > /dev/null
usage() {
    printf "\nUsage: use-python <py-#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Python root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Python version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"

"$ROOT/current/bin/python" --version
EOF
    else
        cat << EOF > "$BIN_DIR/use-python"
#!/usr/bin/env bash

ROOT="$PYTHON_ROOT"

EOF
        cat << 'EOF' >> "$BIN_DIR/use-python"
usage() {
    printf "\nUsage: use-python <py-#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Python root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Python version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"

"$ROOT/current/bin/python" --version
EOF
    fi

    if [[ "$LOCATION_TARGET" == "system" ]]; then sudo chmod +x "$BIN_DIR/use-python"; else chmod +x "$BIN_DIR/use-python"; fi
}

# =====:: scala

print_scala_guide() {
    cat << 'EOF'

# ================================================== #

Usage:

1. Reload shell:

    source ~/.bashrc

2. Check Scala version:

    scala -version

3. Change Scala version:

    use-scala [1]          # Show Usage

    use-scala scala-2.12   # Switch Version

# ================================================== #

EOF
}

setup_scala() {
    # check java
    if [[ ! -d "$JAVA_ROOT/current" ]]; then
        echo "[WARN] Java not found."
        echo "[WARN] Scala will be installed, but CANNOT run until Java is installed."
        echo "[WARN] Recommended: $0 -j 17"
    fi

    # check version
    local version="$1" # 2.12.18 | 2.13.12 | 3.7.4
    [[ -n "$version" ]] || {
        echo "[ERROR] Missing Scala version"
        return 1
    }

    # download
    printf "\n===== Installing Scala %s =====\n" "$version"
    local tmp_dir; tmp_dir="$(mktemp -d)"; trap "rm -rf \'$tmp_dir\'" RETURN INT TERM
    local name="scala-${version%.*}"
    local archive="$tmp_dir/${name}.tgz"
    local url="https://github.com/scala/scala/releases/download/v${version}/scala-${version}.tgz"

    local major="${version%%.*}"
    if [[ ${major} -eq 3 ]]; then
        archive="$tmp_dir/${name}.tar.gz"
        url="https://github.com/scala/scala3/releases/download/${version}/scala3-${version}.tar.gz"
    fi

    smart_download "$url" "$archive" || {
        echo "[ERROR] Download failed"
        return 1
    }
    [[ "$ONLY_DOWNLOAD" == "true" ]] && { printf "[INFO] Only download mode: skipped installation for %s\n" "$name"; return 0; }

    # extract
    tar -xzf "$archive" -C "$tmp_dir"
    local extracted="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    [[ -n "$extracted" ]] || {
        echo "[ERROR] Extraction failed"
        return 1
    }

    # move
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$SCALA_ROOT"
        sudo chown -R "$USER:$USER" "$SCALA_ROOT"
    else
        mkdir -p "$SCALA_ROOT"
    fi
    local setup_dir="$SCALA_ROOT/$name"
    rm -rf "$setup_dir"
    mv "$extracted" "$setup_dir"

    # symlink
    ln -sfn "$setup_dir" "$SCALA_ROOT/current"

    # done
    printf "\n[INFO] Scala %s installed and set as current\n\n" "$version"
    "$SCALA_ROOT/current/bin/scala" -version 2>&1 | head -n1
}

install_scala_tool() {
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        cat << EOF | sudo tee "$BIN_DIR/use-scala" > /dev/null
#!/usr/bin/env bash

ROOT="$SCALA_ROOT"

EOF
        cat << 'EOF' | sudo tee -a "$BIN_DIR/use-scala" > /dev/null

usage() {
    printf "\nUsage: use-scala <scala-#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Scala root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Scala version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"

"$ROOT/current/bin/scala" -version
EOF
    else
        cat << EOF > "$BIN_DIR/use-scala"
#!/usr/bin/env bash

ROOT="$SCALA_ROOT"

EOF
        cat << 'EOF' >> "$BIN_DIR/use-scala"

usage() {
    printf "\nUsage: use-scala <scala-#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Scala root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Scala version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"

"$ROOT/current/bin/scala" -version
EOF
    fi

    if [[ "$LOCATION_TARGET" == "system" ]]; then sudo chmod +x "$BIN_DIR/use-scala"; else chmod +x "$BIN_DIR/use-scala"; fi
}

# =====:: hadoop

print_hadoop_guide() {
    cat << 'EOF'

# ================================================== #

Usage:

1. Reload shell:

    source ~/.bashrc

2. Check Hadoop version:

    hadoop version

3. Change Hadoop version:

    use-hadoop [1]             # Show Usage

    use-hadoop hadoop-3.3.6    # Switch Version

4. Format HDFS (first time only):

    hdfs namenode -format

5. Start Hadoop:

    start-dfs.sh
    start-yarn.sh

# ================================================== #

EOF
}

setup_hadoop() {
    # check java
    if [[ ! -d "$JAVA_ROOT/current" ]]; then
        echo "[WARN] Java not found."
        echo "[WARN] Hadoop will be installed, but services cannot start without Java."
        echo "[WARN] Recommended: $0 -j 17"
    fi

    # check version
    local version="$1"
    [[ -n "$version" ]] || {
        echo "[ERROR] Missing Hadoop version"
        return 1
    }
    if ! echo "$version" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
        echo "[ERROR] Invalid format. Expected: <major.minor.patch> (e.g., 3.3.6)" >&2
        return 1
    fi

    # download
    printf "\n===== Installing Hadoop %s =====\n" "$version"
    local tmp_dir; tmp_dir="$(mktemp -d)"; trap "rm -rf \'$tmp_dir\'" RETURN INT TERM
    local name="hadoop-${version}"
    local archive="$tmp_dir/${name}.tar.gz"
    local url="https://downloads.apache.org/hadoop/common/hadoop-${version}/hadoop-${version}.tar.gz"
    smart_download "$url" "$archive" || {
        echo "[ERROR] Download failed"
        return 1
    }
    [[ "$ONLY_DOWNLOAD" == "true" ]] && { printf "[INFO] Only download mode: skipped installation for %s\n" "$name"; return 0; }

    # extract
    tar -xzf "$archive" -C "$tmp_dir"
    local extracted="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    [[ -n "$extracted" ]] || {
        echo "[ERROR] Extraction failed"
        return 1
    }

    # move
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$HADOOP_ROOT"
        sudo chown -R "$USER:$USER" "$HADOOP_ROOT"
    else
        mkdir -p "$HADOOP_ROOT"
    fi
    local setup_dir="$HADOOP_ROOT/$name"
    rm -rf "$setup_dir"
    mv "$extracted" "$setup_dir"

    # configure
    configure_hadoop "$setup_dir"

    # symlink
    ln -sfn "$setup_dir" "$HADOOP_ROOT/current"

    # done
    printf "\n[INFO] Hadoop %s installed and set as current\n\n" "$version"
    "$HADOOP_ROOT/current/bin/hadoop" version | head -n1
}

install_hadoop_tool() {
        if [[ "$LOCATION_TARGET" == "system" ]]; then
        cat << EOF | sudo tee "$BIN_DIR/use-hadoop" > /dev/null
#!/usr/bin/env bash

ROOT="$HADOOP_ROOT"

EOF
        cat << 'EOF' | sudo tee -a "$BIN_DIR/use-hadoop" > /dev/null
usage() {
    printf "\nUsage: use-hadoop <hadoop-#.#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Hadoop root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Hadoop version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"
EOF
        cat << EOF | sudo tee -a "$BIN_DIR/use-hadoop" > /dev/null
if [[ ! -d "$JAVA_ROOT/current" ]]; then
    echo "[WARN] Java not found. Hadoop commands may fail."
fi
"\$ROOT/current/bin/hadoop" version | head -n1
EOF
    else
        cat << EOF > "$BIN_DIR/use-hadoop"
#!/usr/bin/env bash

ROOT="$HADOOP_ROOT"

EOF
        cat << 'EOF' >> "$BIN_DIR/use-hadoop"
usage() {
    printf "\nUsage: use-hadoop <hadoop-#.#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Hadoop root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Hadoop version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"
EOF
        cat << EOF >> "$BIN_DIR/use-hadoop"
if [[ ! -d "$JAVA_ROOT/current" ]]; then
    echo "[WARN] Java not found. Hadoop commands may fail."
fi
"\$ROOT/current/bin/hadoop" version | head -n1
EOF
    fi
    if [[ "$LOCATION_TARGET" == "system" ]]; then sudo chmod +x "$BIN_DIR/use-hadoop"; else chmod +x "$BIN_DIR/use-hadoop"; fi
}

configure_hadoop() {
    local setup_dir="$1"
    local conf_dir="$setup_dir/etc/hadoop"
    echo ">> Configuring Hadoop for cluster with $WORKER_NODES workers"

    # Configure hadoop-env.sh
    if [[ -f "$conf_dir/hadoop-env.sh" ]]; then
        # Set JAVA_HOME if Java is installed
        if [[ -d "$JAVA_ROOT/current" ]]; then
            sed -i "
            /^# export JAVA_HOME=/{
                n
                /^export[[:space:]]\\+JAVA_HOME=/{
                    s|.*|export JAVA_HOME=\"$JAVA_ROOT/current\"|
                    b
                }
                i export JAVA_HOME=\"$JAVA_ROOT/current\"
            }" "$conf_dir/hadoop-env.sh"
        fi

        # Set HADOOP_OPTS
        sed -i "
        /^# export HADOOP_OPTS=/{
            :a
            n
            /^#/ ba
            /^[[:space:]]*$/{
                i export HADOOP_OPTS=\"-Djava.net.preferIPv4Stack=true\"
            }
        }" "$conf_dir/hadoop-env.sh"
    fi

    # Create required directories
    local tmp_dir="$HADOOP_TMP_DIR"
    local namenode_dir="$HADOOP_NAMENODE_DIR"
    local datanode_dir="$HADOOP_DATANODE_DIR"
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$tmp_dir" "$namenode_dir" "$datanode_dir"
        sudo chown -R "$USER:$USER" "$tmp_dir" "$namenode_dir" "$datanode_dir"
        sudo chmod -R 755 "$tmp_dir" "$namenode_dir" "$datanode_dir"
    else
        mkdir -p "$tmp_dir" "$namenode_dir" "$datanode_dir"
        chmod -R 755 "$tmp_dir" "$namenode_dir" "$datanode_dir"
    fi

    # Calculate HDFS block size in bytes
    local block_size_bytes=$((HDFS_BLOCK_SIZE_MB * 1024 * 1024))

    # Create core-site.xml
    cat << CORE_SITE > "$conf_dir/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://${MASTER_HOST}:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>${tmp_dir}</value>
    </property>
    <property>
        <name>io.file.buffer.size</name>
        <value>131072</value>
        <description>Buffer size for read/write operations (128KB)</description>
    </property>
</configuration>
CORE_SITE

    # Create hdfs-site.xml
    cat << HDFS_SITE > "$conf_dir/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>${HDFS_REPLICATION}</value>
        <description>Replication factor (min 2 for ${WORKER_NODES}-node cluster)</description>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>${block_size_bytes}</value>
        <description>Block size: ${HDFS_BLOCK_SIZE_MB}MB</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://${namenode_dir}</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://${datanode_dir}</value>
    </property>
    <property>
        <name>dfs.namenode.handler.count</name>
        <value>20</value>
        <description>NameNode handler threads for ${WORKER_NODES} workers</description>
    </property>
    <property>
        <name>dfs.datanode.handler.count</name>
        <value>10</value>
        <description>DataNode handler threads</description>
    </property>
</configuration>
HDFS_SITE

    # Create mapred-site.xml
    cat << MAPRED_SITE > "$conf_dir/mapred-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>4096</value>
    </property>
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx1638m</value>
    </property>
    <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx3276m</value>
    </property>
</configuration>
MAPRED_SITE

    # Create yarn-site.xml
    cat << YARN_SITE > "$conf_dir/yarn-site.xml"
<?xml version="1.0"?>
<configuration>
    <!-- ResourceManager configuration -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>${MASTER_HOST}</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
    </property>

    <!-- NodeManager configuration (per worker: ${WORKER_VCORES} cores, ${WORKER_MEMORY_MB}MB RAM) -->
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>${YARN_NM_MEMORY_MB}</value>
        <description>Total memory per NodeManager (${WORKER_NODES} workers)</description>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>${YARN_NM_VCORES}</value>
        <description>Total cores per NodeManager</description>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <!-- Container limits -->
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>${YARN_CONTAINER_MIN_MEMORY_MB}</value>
        <description>Minimum container memory</description>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>${YARN_CONTAINER_MAX_MEMORY_MB}</value>
        <description>Maximum container memory (80% of NM memory)</description>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-vcores</name>
        <value>${YARN_CONTAINER_MIN_VCORES}</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>${YARN_CONTAINER_MAX_VCORES}</value>
    </property>

    <!-- Performance tuning -->
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
        <description>Disable virtual memory checking</description>
    </property>
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>true</value>
    </property>
</configuration>
YARN_SITE

    echo ">> Hadoop configured for:"
    echo "   - HDFS: ${HDFS_REPLICATION}x replication, ${HDFS_BLOCK_SIZE_MB}MB blocks"
    echo "   - YARN: ${WORKER_NODES} workers × ${YARN_NM_VCORES} cores × ${YARN_NM_MEMORY_MB}MB"
}

# =====:: spark

print_spark_guide() {
    cat << 'EOF'

# ================================================== #

Usage:

1. Reload shell:

    source ~/.bashrc

2. Check Spark version:

    spark-submit --version

3. Change Spark version:

    use-spark [1]            # Show Usage

    use-spark spark-3.5.7    # Switch Version

4. Start Spark Shell:

    spark-shell              # Scala
    pyspark                  # Python

5. Start Spark Services:

    start-master.sh                           # Start Master
    start-worker.sh spark://localhost:7077    # Start Worker

# ================================================== #

EOF
}

setup_spark() {
    # check java
    if [[ ! -d "$JAVA_ROOT/current" ]]; then
        echo "[WARN] Java not found."
        echo "[WARN] Spark will be installed, but cannot run without Java."
        echo "[WARN] Recommended: $0 -j 17"
    fi

    # check version
    local version="$1"
    [[ -n "$version" ]] || {
        echo "[ERROR] Missing Spark version"
        return 1
    }
    if ! echo "$version" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
        echo "[ERROR] Invalid format. Expected: <major.minor.patch> (e.g., 3.5.7)" >&2
        return 1
    fi

    # Spark 4.x uses Hadoop 3.4 and Scala 2.13
    local hadoop_version="3.3"
    local scala_version="2.12"
    local major="${version%%.*}"
    if [[ ${major} -ge 4 ]]; then
        hadoop_version="3.4"
        scala_version="2.13"
    fi

    # download
    printf "\n===== Installing Spark %s (Hadoop %s, Scala %s) =====\n" "$version" "$hadoop_version" "$scala_version"
    local tmp_dir; tmp_dir="$(mktemp -d)"; trap "rm -rf \'$tmp_dir\'" RETURN INT TERM
    local name="spark-${version}"
    local archive="$tmp_dir/${name}.tgz"
    local url="https://downloads.apache.org/spark/spark-${version}/spark-${version}-bin-hadoop3.tgz"
    smart_download "$url" "$archive" || {
        echo "[ERROR] Download failed"
        return 1
    }
    [[ "$ONLY_DOWNLOAD" == "true" ]] && { printf "[INFO] Only download mode: skipped installation for %s\n" "$name"; return 0; }

    # extract
    tar -xzf "$archive" -C "$tmp_dir"
    local extracted="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n1)"
    [[ -n "$extracted" ]] || {
        echo "[ERROR] Extraction failed"
        return 1
    }

    # move
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$SPARK_ROOT"
        sudo chown -R "$USER:$USER" "$SPARK_ROOT"
    else
        mkdir -p "$SPARK_ROOT"
    fi
    local setup_dir="$SPARK_ROOT/$name"
    rm -rf "$setup_dir"
    mv "$extracted" "$setup_dir"

    # configure
    configure_spark "$setup_dir"

    # symlink
    ln -sfn "$setup_dir" "$SPARK_ROOT/current"

    # done
    printf "\n[INFO] Spark %s installed and set as current\n\n" "$version"
    "$SPARK_ROOT/current/bin/spark-submit" --version 2>&1 | head -n8
}

configure_spark() {
    local setup_dir="$1"
    local conf_dir="$setup_dir/conf"
    local spark_local_dir="$SPARK_LOCAL_DIR"
    local spark_log_dir="$SPARK_LOG_DIR"

    echo ">> Configuring Spark for ${WORKER_NODES}-node cluster"
    if [[ "$LOCATION_TARGET" == "system" ]]; then
        sudo mkdir -p "$spark_local_dir"
        sudo chown -R "$USER:$USER" "$spark_local_dir"
        sudo chmod -R 755 "$spark_local_dir"
    else
        mkdir -p "$spark_local_dir"
        chmod -R 755 "$spark_local_dir"
    fi

    # Create or use existing spark-env.sh
    if [[ ! -f "$conf_dir/spark-env.sh" ]]; then
        cp "$conf_dir/spark-env.sh.template" "$conf_dir/spark-env.sh" 2>/dev/null || touch "$conf_dir/spark-env.sh"
    fi
    chmod +x "$conf_dir/spark-env.sh"

    # Set JAVA_HOME
    if [[ -d "$JAVA_ROOT/current" ]]; then
        if grep -q '^export[[:space:]]\+JAVA_HOME=' "$conf_dir/spark-env.sh"; then
            sed -i "s|^export[[:space:]]\+JAVA_HOME=.*|export JAVA_HOME=\"$JAVA_ROOT/current\"|" \
                "$conf_dir/spark-env.sh"
        else
            echo "export JAVA_HOME=\"$JAVA_ROOT/current\"" >> "$conf_dir/spark-env.sh"
        fi
    fi

    # Set HADOOP_CONF_DIR
    if [[ -d "$HADOOP_ROOT/current" ]]; then
        if grep -q '^export[[:space:]]\+HADOOP_CONF_DIR=' "$conf_dir/spark-env.sh"; then
            sed -i "s|^export[[:space:]]\+HADOOP_CONF_DIR=.*|export HADOOP_CONF_DIR=\"$HADOOP_ROOT/current/etc/hadoop\"|" \
                "$conf_dir/spark-env.sh"
        else
            echo "export HADOOP_CONF_DIR=\"$HADOOP_ROOT/current/etc/hadoop\"" >> "$conf_dir/spark-env.sh"
        fi
    fi

    # Set SPARK_LOCAL_DIRS
    if grep -q '^export[[:space:]]\+SPARK_LOCAL_DIRS=' "$conf_dir/spark-env.sh"; then
        sed -i "s|^export[[:space:]]\+SPARK_LOCAL_DIRS=.*|export SPARK_LOCAL_DIRS=\"${spark_local_dir}\"|" \
            "$conf_dir/spark-env.sh"
    else
        echo "export SPARK_LOCAL_DIRS=\"${spark_local_dir}\"" >> "$conf_dir/spark-env.sh"
    fi

    # Add Spark Standalone cluster configuration to spark-env.sh
    cat >> "$conf_dir/spark-env.sh" << SPARK_ENV

# ==== Spark Standalone Cluster Configuration ====
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT}

# Worker configuration (per node: ${WORKER_VCORES} cores, ${WORKER_MEMORY_MB}MB)
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
export SPARK_WORKER_INSTANCES=${SPARK_WORKER_INSTANCES}

# Daemon memory
export SPARK_DAEMON_MEMORY=512m
SPARK_ENV

    # Create spark-defaults.conf
    if [[ ! -f "$conf_dir/spark-defaults.conf" ]]; then
        cat << SPARK_DEFAULTS > "$conf_dir/spark-defaults.conf"
# ============================================================
# Spark Configuration for ${WORKER_NODES}-Worker Cluster
# Total Resources: ${WORKER_NODES} workers × ${WORKER_VCORES} cores × ${WORKER_MEMORY_MB}MB
# ============================================================

# ==== Cluster Configuration ====
# Standalone mode: spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}
# YARN mode: set master to 'yarn' when submitting
spark.master                            spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}

# ==== Executor Configuration (Standalone & YARN) ====
# Strategy: ${SPARK_EXECUTOR_INSTANCES} executors × ${SPARK_EXECUTOR_CORES} cores × ${SPARK_EXECUTOR_MEMORY}
spark.executor.cores                    ${SPARK_EXECUTOR_CORES}
spark.executor.memory                   ${SPARK_EXECUTOR_MEMORY}
spark.executor.instances                ${SPARK_EXECUTOR_INSTANCES}

# Executor memory overhead (10% of executor memory)
spark.executor.memoryOverhead           410m

# ==== Driver Configuration ====
spark.driver.cores                      ${SPARK_DRIVER_CORES}
spark.driver.memory                     ${SPARK_DRIVER_MEMORY}
spark.driver.memoryOverhead             205m

# ==== YARN-specific Configuration ====
# Uncomment these when running on YARN
# spark.yarn.executor.memoryOverhead    410m
# spark.yarn.driver.memoryOverhead      205m
# spark.yarn.am.memory                  ${SPARK_YARN_AM_MEMORY}

# ==== Parallelism & Partitioning ====
# Rule: 2-3× total cores = 2 × (${WORKER_NODES} × ${WORKER_VCORES}) = $((WORKER_NODES * WORKER_VCORES * 2))
spark.default.parallelism               ${SPARK_DEFAULT_PARALLELISM}
spark.sql.shuffle.partitions            ${SPARK_SQL_SHUFFLE_PARTITIONS}

# ==== Memory Management ====
spark.memory.fraction                   ${SPARK_MEMORY_FRACTION}
spark.memory.storageFraction            0.5

# ==== Serialization ====
spark.serializer                        org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max         64m
spark.kryoserializer.buffer             1m

# ==== Shuffle Optimization ====
spark.shuffle.service.enabled           false
spark.shuffle.file.buffer               32k
spark.reducer.maxSizeInFlight           48m
spark.shuffle.io.retryWait              5s
spark.shuffle.io.maxRetries             3

# ==== Network ====
spark.network.timeout                   300s
spark.rpc.askTimeout                    300s
spark.rpc.lookupTimeout                 120s

# ==== Dynamic Allocation (Disabled for benchmark consistency) ====
spark.dynamicAllocation.enabled         false
spark.speculation                       false

# ==== Event Logging & History ====
spark.eventLog.enabled                  true
spark.eventLog.dir                      ${spark_log_dir}
spark.history.fs.logDirectory           ${spark_log_dir}

# ==== UI ====
spark.ui.port                           4040
spark.ui.retainedJobs                   100
spark.ui.retainedStages                 100

# ==== Garbage Collection ====
spark.executor.extraJavaOptions         -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
spark.driver.extraJavaOptions           -XX:+UseG1GC

# ==== Broadcast ====
spark.broadcast.blockSize               4m

# ==== Local Directory ====
spark.local.dir                         ${spark_local_dir}
SPARK_DEFAULTS
    fi

    echo ">> Spark configured for:"
    echo "   - Standalone: ${WORKER_NODES} workers × ${SPARK_WORKER_CORES} cores × ${SPARK_WORKER_MEMORY}"
    echo "   - Executors: ${SPARK_EXECUTOR_INSTANCES} total × ${SPARK_EXECUTOR_CORES} cores × ${SPARK_EXECUTOR_MEMORY}"
    echo "   - YARN: Compatible (change spark.master to 'yarn' when submitting)"
}

install_spark_tool() {
        if [[ "$LOCATION_TARGET" == "system" ]]; then
        cat << EOF | sudo tee "$BIN_DIR/use-spark" > /dev/null
#!/usr/bin/env bash

ROOT="$SPARK_ROOT"

EOF
        cat << 'EOF' | sudo tee -a "$BIN_DIR/use-spark" > /dev/null
usage() {
    printf "\nUsage: use-spark <spark-#.#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Spark root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Spark version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"
EOF
        cat << EOF | sudo tee -a "$BIN_DIR/use-spark" > /dev/null
if [[ ! -d "$JAVA_ROOT/current" ]]; then
    echo "[WARN] Java not found. Spark commands may fail."
fi
"\$ROOT/current/bin/spark-submit" --version 2>&1 | head -n8
EOF
    else
        cat << EOF > "$BIN_DIR/use-spark"
#!/usr/bin/env bash

ROOT="$SPARK_ROOT"

EOF
        cat << 'EOF' >> "$BIN_DIR/use-spark"
usage() {
    printf "\nUsage: use-spark <spark-#.#.#>\n\nAvailable versions:\n"
    for d in "$ROOT"/*; do
        [ -d "$d" ] && [ "$(basename "$d")" != "current" ] && echo "    $(basename "$d")"
    done
    printf "\n"
}

[[ -d "$ROOT" ]] || {
    echo "Spark root not found: $ROOT"
    exit 1
}

[[ $# -eq 1 ]] || {
    usage
    exit 1
}

if [ ! -d "$ROOT/$1" ]; then
    echo "Spark version not found: $1"
    usage
    exit 1
fi

ln -sfn "$ROOT/$1" "$ROOT/current"
EOF
        cat << EOF >> "$BIN_DIR/use-spark"
if [[ ! -d "$JAVA_ROOT/current" ]]; then
    echo "[WARN] Java not found. Spark commands may fail."
fi
"\$ROOT/current/bin/spark-submit" --version 2>&1 | head -n8
EOF
    fi
    if [[ "$LOCATION_TARGET" == "system" ]]; then sudo chmod +x "$BIN_DIR/use-spark"; else chmod +x "$BIN_DIR/use-spark"; fi
}

# =====:: main

main() {
    local -a JAVA_VERSIONS=()
    local -a PYTHON_VERSIONS=()
    local -a PYPY_VERSIONS=()
    local -a SCALA_VERSIONS=()
    local -a HADOOP_VERSIONS=()
    local -a SPARK_VERSIONS=()

    [[ $# -eq 0 ]] && {
        usage
        exit 0
    }
    while [[ $# -gt 0 ]]; do
        case "$1" in
            '-h'|'--help')
                usage
                exit 0
                ;;
            '-j'|'--java')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for $1"; exit 1; }
                JAVA_VERSIONS+=("$2")
                shift 2
                continue
                ;;
            '-p'|'--python')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for $1"; exit 1; }
                PYTHON_VERSIONS+=("$2")
                shift 2
                continue
                ;;
            '-P'|'--pypy')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for $1"; exit 1; }
                PYPY_VERSIONS+=("$2")
                shift 2
                continue
                ;;
            '-s'|'--scala')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for $1"; exit 1; }
                SCALA_VERSIONS+=("$2")
                shift 2
                continue
                ;;
            '-H'|'--hadoop')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for $1"; exit 1; }
                HADOOP_VERSIONS+=("$2")
                shift 2
                continue
                ;;
            '-S'|'--spark')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for $1"; exit 1; }
                SPARK_VERSIONS+=("$2")
                shift 2
                continue
                ;;
                        '--offline')
                OFFLINE_MODE=true
                shift
                continue
                ;;
            '--only-download')
                ONLY_DOWNLOAD=true
                shift
                continue
                ;;
            '--rc')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for --rc"; exit 1; }
                RC_TARGET="$2"
                shift 2
                continue
                ;;
            '-l'|'--location')
                [[ -n "${2:-}" ]] || { echo "[ERROR] Missing value for --location"; exit 1; }
                LOCATION_TARGET="$2"
                shift 2
                continue
                ;;
            *)
                printf "\n[ERROR] Unknown option: %s\n\n" "$1"
                usage
                exit 1
                ;;
        esac
    done
    case "$LOCATION_TARGET" in
        'system'|'local') ;;
        *)
            echo "[ERROR] Invalid --location value: '$LOCATION_TARGET'"
            echo "[INFO] Valid values: system | local"
            exit 1
            ;;
    esac
    apply_location_target

    ### common setup
    install_dependencies
    ensure_bin

    local rc_java="$(resolve_rc)"
    local rc_py="$(resolve_rc)"
    local rc_scala="$(resolve_rc)"
    local rc_cluster="$(resolve_rc)"

    [[ -z "$rc_java" ]] && rc_java="$HOME/.bashrc"
    [[ -z "$rc_py" ]] && rc_py="$HOME/.bashrc"
    [[ -z "$rc_scala" ]] && rc_scala="$HOME/.bashrc"
    [[ -z "$rc_cluster" ]] && rc_cluster="$HOME/.bashrc"

    ### java
    ((${#JAVA_VERSIONS[@]})) && printf "\n[INFO] Requested Java versions: %s\n" "${JAVA_VERSIONS[*]}"
    for v in "${JAVA_VERSIONS[@]}"; do
        setup_java "$v"
    done

    ((${#JAVA_VERSIONS[@]})) && {
        install_java_tool
        ensure_managed_block "$rc_java" \
            "# >>> Java environment (managed)" \
            "# <<< Java environment" \
            "export JAVA_HOME=\"$JAVA_ROOT/current\"" \
            'export PATH="$JAVA_HOME/bin:$PATH"'
        print_java_guide
        if ! source "$rc_java" 2>/dev/null; then
            echo "[WARN] Failed to source $rc_java - please check for syntax errors"
        fi
    }

    ### python and pypy
    ((${#PYTHON_VERSIONS[@]})) && printf "\n[INFO] Requested Python versions: %s\n" "${PYTHON_VERSIONS[*]}"
    for v in "${PYTHON_VERSIONS[@]}"; do
        setup_python "$v"
    done

    ((${#PYPY_VERSIONS[@]})) && printf "\n[INFO] Requested PyPy versions: %s\n" "${PYPY_VERSIONS[*]}"
    for v in "${PYPY_VERSIONS[@]}"; do
        setup_pypy "$v"
    done

    (( ${#PYTHON_VERSIONS[@]} + ${#PYPY_VERSIONS[@]} > 0 )) && {
        install_python_tool
        ensure_managed_block "$rc_py" \
            "# >>> Python environment (managed)" \
            "# <<< Python environment" \
            "export PATH=\"$PYTHON_ROOT/current/bin:\$PATH\""
        print_python_guide
        if ! source "$rc_py" 2>/dev/null; then
            echo "[WARN] Failed to source $rc_py - please check for syntax errors"
        fi
    }

    ### scala
    ((${#SCALA_VERSIONS[@]})) && printf "\n[INFO] Requested Scala versions: %s\n" "${SCALA_VERSIONS[*]}"
    for v in "${SCALA_VERSIONS[@]}"; do
        setup_scala "$v"
    done

    ((${#SCALA_VERSIONS[@]})) && {
        install_scala_tool
        ensure_managed_block "$rc_scala" \
            "# >>> Scala environment (managed)" \
            "# <<< Scala environment" \
            "export SCALA_HOME=\"$SCALA_ROOT/current\"" \
            'export PATH="$SCALA_HOME/bin:$PATH"'
        print_scala_guide
        if ! source "$rc_scala" 2>/dev/null; then
            echo "[WARN] Failed to source $rc_scala - please check for syntax errors"
        fi
    }

    ### hadoop
    ((${#HADOOP_VERSIONS[@]})) && printf "\n[INFO] Requested Hadoop versions: %s\n" "${HADOOP_VERSIONS[*]}"
    for v in "${HADOOP_VERSIONS[@]}"; do
        setup_hadoop "$v"
    done

    ((${#HADOOP_VERSIONS[@]})) && {
        install_hadoop_tool
        ensure_managed_block "$rc_cluster" \
            "# >>> Hadoop environment (managed)" \
            "# <<< Hadoop environment" \
            "export HADOOP_HOME=\"$HADOOP_ROOT/current\"" \
            'export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"' \
            'export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"' \
            'export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true"' \
            'export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"'
        print_hadoop_guide
        if ! source "$rc_cluster" 2>/dev/null; then
            echo "[WARN] Failed to source $rc_cluster - please check for syntax errors"
        fi
    }

    ### spark
    ((${#SPARK_VERSIONS[@]})) && printf "\n[INFO] Requested Spark versions: %s\n" "${SPARK_VERSIONS[*]}"
    for v in "${SPARK_VERSIONS[@]}"; do
        setup_spark "$v"
    done

    ((${#SPARK_VERSIONS[@]})) && {
        install_spark_tool
        ensure_managed_block "$rc_cluster" \
            "# >>> Spark environment (managed)" \
            "# <<< Spark environment" \
            "export SPARK_HOME=\"$SPARK_ROOT/current\"" \
            'export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"' \
            'export PYSPARK_PYTHON=python3' \
            'export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"'
        print_spark_guide
        if ! source "$rc_cluster" 2>/dev/null; then
            echo "[WARN] Failed to source $rc_cluster - please check for syntax errors"
        fi
    }
}

main "$@"
