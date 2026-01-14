#!/usr/bin/env bash
set -euo pipefail

((BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 3))) || {
    echo "[ERROR] Bash >= 4.3 required"
    exit 1
}

# =====:: global config

readonly CLUSTER_NODES=(
    "192.168.100.101 master"
    "192.168.100.102 worker1"
    "192.168.100.103 worker2"
    "192.168.100.104 worker3"
    "192.168.100.105 worker4"
)

readonly GATEWAY_IP_DEFAULT="192.168.100.1"

# Temporary files tracking for cleanup
declare -a TEMP_FILES=()

# =====:: cleanup handler

cleanup() {
    local exit_code=$?
    if [[ ${#TEMP_FILES[@]} -gt 0 ]]; then
        for tmp in "${TEMP_FILES[@]}"; do
            [[ -f "$tmp" ]] && rm -f "$tmp"
        done
    fi
    return $exit_code
}

trap cleanup EXIT INT TERM

# =====:: guide

usage() {
    cat << EOF

==================================================

Usage: $0 [OPTIONS]

[OPTIONS]:
    -h, --help                      :Show help (Default)

    -r, --role <master|worker>      :Node role
    -n, --hostname <name>           :Set hostname

    -i, --static-ip <CIDR>          :Static IP (e.g. 192.168.100.101/24)
    -g, --gateway <IP>              :Gateway (required if --static-ip)
    --online                        :Host-only + NAT (Internet)

    --sync                          :Sync SSH keys to workers (requires --role master)

Examples:
    Master (offline mode):
        $0 -r master -n master \
            -i 192.168.100.101/24 -g 192.168.100.1

    Worker (online mode with internet):
        $0 -r worker -n worker1 \
            -i 192.168.100.102/24 -g 192.168.100.1 --online

    Master with SSH key sync:
        $0 -r master -n master \
            -i 192.168.100.101/24 -g 192.168.100.1 --sync

==================================================

EOF
}

# =====:: validation helpers

require_sudo() {
    sudo -v || {
        echo "[ERROR] sudo required"
        exit 1
    }
}

validate_hostname() {
    local hostname="$1"
    [[ -z "$hostname" ]] && {
        echo "[ERROR] Hostname cannot be empty"
        return 1
    }
    # Hostname must: start with alphanumeric, contain only alphanumeric and hyphens,
    # not end with hyphen, max 63 chars
    if [[ ! "$hostname" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$ ]]; then
        echo "[ERROR] Invalid hostname: $hostname"
        echo "        Hostname must:"
        echo "        - Start with alphanumeric character"
        echo "        - Contain only alphanumeric and hyphens"
        echo "        - Not end with hyphen"
        echo "        - Be 1-63 characters long"
        return 1
    fi
    return 0
}

# =====:: static ip

setup_ip() {
    # Usage: setup_ip "192.168.100.101/24" "192.168.100.1" [online|offline]
    local static_ip="$1"
    local gateway_ip="$2"
    local mode="${3:-offline}"
    [[ -z "$static_ip" ]] && return 0
    [[ "$static_ip" != */* ]] && {
        echo "[ERROR] static_ip must include CIDR (e.g. 192.168.100.101/24)"
        return 1
    }
    [[ -z "$gateway_ip" ]] && {
        echo "[ERROR] gateway is required when using static IP"
        return 1
    }
    [[ ! "$mode" =~ ^(offline|online)$ ]] && {
        echo "[ERROR] mode must be 'offline' or 'online'"
        return 1
    }

    # Detect interfaces correctly
    local default_iface="$(ip route show default 2>/dev/null | awk '{print $5; exit}')"
    local ifaces=()
    mapfile -t ifaces < <(
        ip -o link show \
        | awk -F': ' '$2!="lo" && $2!~/^(docker|br-|veth)/{print $2}'
    )
    [[ "${#ifaces[@]}" -eq 0 ]] && {
        echo "[ERROR] No usable network interfaces found"
        return 1
    }
    local cluster_iface=""
    local nat_iface=""
    if [[ "$mode" == "offline" ]]; then
        cluster_iface="${ifaces[0]}"
    else
        [[ -z "$default_iface" ]] && {
            echo "[ERROR] online mode requires a default route (NAT iface)"
            return 1
        }
        nat_iface="$default_iface"
        # Break after finding first interface different from nat_iface
        for i in "${ifaces[@]}"; do
            if [[ "$i" != "$nat_iface" ]]; then
                cluster_iface="$i"
                break
            fi
        done
        [[ -z "$cluster_iface" ]] && {
            echo "[ERROR] Unable to determine cluster interface"
            return 1
        }
        # Verify nat_iface exists in ifaces array
        local nat_found="no"
        for i in "${ifaces[@]}"; do
            [[ "$i" == "$nat_iface" ]] && nat_found="yes"
        done
        [[ "$nat_found" == "no" ]] && {
            echo "[ERROR] NAT interface $nat_iface not found in available interfaces"
            return 1
        }
    fi

    # Validate gateway is in subnet
    if command -v python3 >/dev/null 2>&1; then
        if ! python3 - <<EOF >/dev/null 2>&1
import ipaddress
net = ipaddress.ip_interface("$static_ip").network
if ipaddress.ip_address("$gateway_ip") not in net:
    raise SystemExit(1)
EOF
        then
            echo "[ERROR] Gateway $gateway_ip is not in subnet of $static_ip"
            return 1
        fi
    else
        echo "[ERROR] python3 is required for gateway validation"
        return 1
    fi

    # Decide whether to SKIP
    local current_ip="$(ip -o -4 addr show "$cluster_iface" 2>/dev/null | awk '{print $4}' | head -n1)"
    local current_gateway="$(ip route show default 2>/dev/null | awk '{print $3; exit}')"
    local has_default_route="no"
    local current_default_iface=""
    if ip route show default >/dev/null 2>&1; then
        has_default_route="yes"
        current_default_iface="$(ip route show default | awk '{print $5; exit}')"
    fi

    # Check if netplan config already exists and matches
    local netconfig_file="/etc/netplan/99-static-ip.yaml"
    local config_exists="no"
    [[ -f "$netconfig_file" ]] && config_exists="yes"
    local skip="no"
    if [[ "$mode" == "offline" ]]; then
        # For offline: check IP and gateway match, and config file exists
        if [[ "$current_ip" == "$static_ip" && \
              "$current_gateway" == "$gateway_ip" && \
              "$config_exists" == "yes" ]]; then
            skip="yes"
        fi
    else
        # For online: check cluster IP matches and NAT iface has default route
        if [[ "$current_ip" == "$static_ip" && \
              "$has_default_route" == "yes" && \
              "$current_default_iface" == "$nat_iface" && \
              "$config_exists" == "yes" ]]; then
            skip="yes"
        fi
    fi
    if [[ "$skip" == "yes" ]]; then
        echo "[INFO] Network already configured correctly (skip)"
        return 0
    fi

    # Backup existing netplan config
    if [[ -f "$netconfig_file" ]]; then
        local backup_file="${netconfig_file}.backup.$(date +%s)"
        echo "[INFO] Backing up existing config to $backup_file"
        sudo cp "$netconfig_file" "$backup_file"
    fi

    # Disable cloud-init network config
    if [[ -d /etc/cloud/cloud.cfg.d ]]; then
        sudo tee /etc/cloud/cloud.cfg.d/99-disable-network-config.cfg >/dev/null << EOF
network: {config: disabled}
EOF
    fi

    # Detect renderer
    local renderer="networkd"
    systemctl is-active --quiet NetworkManager && renderer="NetworkManager"

    # Write netplan config
    echo "[INFO] Writing netplan config ($mode)"
    echo "       cluster iface = $cluster_iface"
    [[ -n "$nat_iface" ]] && echo "       nat iface     = $nat_iface"

    if [[ "$mode" == "offline" ]]; then
        sudo tee "$netconfig_file" >/dev/null <<EOF
network:
  version: 2
  renderer: $renderer
  ethernets:
    $cluster_iface:
      dhcp4: false
      dhcp6: false
      addresses:
        - $static_ip
      routes:
        - to: default
          via: $gateway_ip
EOF
    else
        sudo tee "$netconfig_file" >/dev/null <<EOF
network:
  version: 2
  renderer: $renderer
  ethernets:
    $cluster_iface:
      dhcp4: false
      dhcp6: false
      addresses:
        - $static_ip
    $nat_iface:
      dhcp4: true
      dhcp6: false
EOF
    fi

    # Fix permissions for ALL netplan files
    echo "[INFO] Fixing netplan permissions..."
    sudo chown root:root /etc/netplan/*.yaml 2>/dev/null || true
    sudo chmod 600 /etc/netplan/*.yaml 2>/dev/null || true

    # Validate netplan config before applying
    if ! sudo netplan generate 2>/dev/null; then
        echo "[ERROR] Generated netplan config is invalid"
        if [[ -f "${netconfig_file}.backup."* ]]; then
            echo "[INFO] Restoring backup..."
            sudo cp "${netconfig_file}.backup."* "$netconfig_file"
        fi
        return 1
    fi

    # Apply netplan
    echo "[INFO] Applying netplan..."
    if [[ "$renderer" == "NetworkManager" ]]; then
        sudo systemctl reload NetworkManager || {
            echo "[WARN] NetworkManager reload failed, trying restart..."
            sudo systemctl restart NetworkManager || {
                echo "[ERROR] NetworkManager restart failed"
                return 1
            }
        }
        sleep 2
        if ! systemctl is-active --quiet NetworkManager; then
            echo "[ERROR] NetworkManager is not running after configuration"
            return 1
        fi
    else
        sudo netplan apply || {
            echo "[ERROR] netplan apply failed"
            return 1
        }
    fi
    echo "[INFO] Network configured successfully ($mode)"
    echo "[INFO] Reboot is recommended"
}

# =====:: ssh and hostname

setup_ssh_and_hostname() {
    # Usage: setup_ssh_and_hostname <master|worker> <hostname> <array_name>
    local role="$1"
    local new_hostname="$2"
    local array_name="$3"

    [[ -z "$role" || -z "$new_hostname" || -z "$array_name" ]] && {
        echo "[ERROR] Usage: setup_ssh_and_hostname <master|worker> <hostname> <cluster_array_name>"
        return 1
    }
    [[ "$role" != "master" && "$role" != "worker" ]] && {
        echo "[ERROR] role must be 'master' or 'worker'"
        return 1
    }

    # Validate hostname
    validate_hostname "$new_hostname" || return 1

    local -n CLUSTER_NODES_REF="$array_name"
    [[ ${#CLUSTER_NODES_REF[@]} -eq 0 ]] && {
        echo "[ERROR] Cluster node array is empty"
        return 1
    }

    echo "[INFO] Setting up SSH & hostname for role=$role"

    # ssh
    if ! systemctl is-enabled ssh >/dev/null 2>&1; then
        echo "[INFO] Installing OpenSSH..."
        sudo apt-get update -qq
        sudo apt-get install -y openssh-server openssh-client
    fi
    sudo systemctl enable ssh >/dev/null 2>&1
    sudo systemctl start ssh  >/dev/null 2>&1

    local sshd_cfg="/etc/ssh/sshd_config"
    local sshd_backup="${sshd_cfg}.backup.$(date +%s)"

    # Backup sshd_config before modification
    if [[ -f "$sshd_cfg" ]]; then
        sudo cp "$sshd_cfg" "$sshd_backup"
        echo "[INFO] Backed up sshd_config to $sshd_backup"
    fi

    local sshd_changed=0
    declare -A SSHD_OPTIONS=(
        [PubkeyAuthentication]="yes"
        [PasswordAuthentication]="yes"
        [PermitRootLogin]="no"
    )

    for key in "${!SSHD_OPTIONS[@]}"; do
        local desired="${SSHD_OPTIONS[$key]}"
        local current
        current="$(sudo grep -E "^[[:space:]]*#?[[:space:]]*$key[[:space:]]+" "$sshd_cfg" \
            | tail -n 1 | awk '{print $2}')"
        if [[ "$current" == "$desired" ]]; then
            echo "[INFO] sshd: $key already $desired (skip)"
            continue
        fi
        echo "[INFO] sshd: setting $key $desired"
        if sudo grep -qE "^[[:space:]]*#?[[:space:]]*$key[[:space:]]+" "$sshd_cfg"; then
            sudo sed -i -E \
                "s|^[[:space:]]*#?[[:space:]]*$key[[:space:]]+.*|$key $desired|" \
                "$sshd_cfg"
        else
            echo "$key $desired" | sudo tee -a "$sshd_cfg" >/dev/null
        fi
        sshd_changed=1
    done

    # Validate and reload sshd config
    if sudo sshd -t 2>/dev/null; then
        if [[ $sshd_changed -eq 1 ]]; then
            sudo systemctl reload ssh
            echo "[INFO] sshd_config valid, reloading ssh"
        fi
    else
        echo "[ERROR] sshd_config validation failed"
        if [[ -f "$sshd_backup" ]]; then
            echo "[INFO] Restoring backup..."
            sudo cp "$sshd_backup" "$sshd_cfg"
            sudo systemctl reload ssh
        fi
        return 1
    fi

    # hostname
    local current_hostname
    current_hostname="$(hostname)"

    if [[ "$current_hostname" != "$new_hostname" ]]; then
        echo "[INFO] Setting hostname: $new_hostname"
        sudo hostnamectl set-hostname "$new_hostname"
    else
        echo "[INFO] Hostname already set: $new_hostname"
    fi

    # hosts file
    local hosts_file="/etc/hosts"
    local hosts_backup="${hosts_file}.backup.$(date +%s)"

    # Backup hosts file
    sudo cp "$hosts_file" "$hosts_backup"
    echo "[INFO] Backed up hosts file to $hosts_backup"

    local block_start="# >>> Cluster IP List"
    local block_end="# <<< Cluster IP List"

    # Comment out 127.0.1.1 entry for this hostname
    if grep -qE "^[[:space:]]*127\.0\.1\.1[[:space:]]+$new_hostname" "$hosts_file"; then
        sudo sed -i -E \
            "s|^[[:space:]]*127\.0\.1\.1[[:space:]]+$new_hostname|# 127.0.1.1 $new_hostname|" \
            "$hosts_file"
    fi

    local cluster_block
    cluster_block="$(printf "%s\n" "${CLUSTER_NODES_REF[@]}")"
    cluster_block="${cluster_block%$'\n'}"

    if grep -qF "$block_start" "$hosts_file"; then
        # Update existing block using a temp file
        local hosts_tmp
        hosts_tmp=$(mktemp)
        TEMP_FILES+=("$hosts_tmp")

        sudo awk -v start="$block_start" -v end="$block_end" -v block="$cluster_block" '
            BEGIN {inblock=0}
            $0==start {print start; print block; inblock=1; next}
            $0==end   {print end; inblock=0; next}
            !inblock  {print}
        ' "$hosts_file" > "$hosts_tmp"

        sudo cp "$hosts_tmp" "$hosts_file"
    else
        # Add new block
        sudo sed -i -e '$a\' "$hosts_file"
        sudo tee -a "$hosts_file" >/dev/null <<EOF

$block_start
$cluster_block
$block_end
EOF
    fi

    echo "[INFO] SSH & hostname setup completed for $role"
}

# =====:: ssh key sync

sync_ssh_keys() {
    # Usage: sync_ssh_keys <array_name> <current_user>
    local array_name="$1"
    local current_user="${2:-$USER}"

    [[ -z "$array_name" ]] && {
        echo "[ERROR] Usage: sync_ssh_keys <cluster_array_name> [username]"
        return 1
    }

    local -n CLUSTER_NODES_REF="$array_name"
    [[ ${#CLUSTER_NODES_REF[@]} -eq 0 ]] && {
        echo "[ERROR] Cluster node array is empty"
        return 1
    }

    echo "[INFO] Starting SSH key synchronization"
    echo "[INFO] Current user: $current_user"

    local ssh_dir="$HOME/.ssh"
    local private_key="$ssh_dir/id_rsa"
    local public_key="$ssh_dir/id_rsa.pub"
    local authorized_keys="$ssh_dir/authorized_keys"
    local known_hosts="$ssh_dir/known_hosts"

    if [[ ! -d "$ssh_dir" ]]; then
        echo "[INFO] Creating $ssh_dir directory"
        mkdir -p "$ssh_dir"
        chmod 700 "$ssh_dir"
    fi

    if [[ ! -f "$private_key" ]]; then
        echo "[INFO] Generating SSH key pair..."
        ssh-keygen -t rsa -b 4096 -f "$private_key" -N "" -C "${current_user}@master"
        echo "[INFO] SSH key pair generated successfully"
    else
        echo "[INFO] SSH key already exists (skip generation)"
    fi

    chmod 600 "$private_key" 2>/dev/null || {
        echo "[ERROR] Failed to set permissions on private key"
        return 1
    }
    chmod 644 "$public_key" 2>/dev/null || {
        echo "[ERROR] Failed to set permissions on public key"
        return 1
    }

    [[ ! -f "$authorized_keys" ]] && touch "$authorized_keys"
    chmod 600 "$authorized_keys"

    local master_pubkey
    master_pubkey="$(cat "$public_key")"

    if ! grep -qxF "$master_pubkey" "$authorized_keys" 2>/dev/null; then
        echo "[INFO] Adding master's public key to its own authorized_keys"
        echo "$master_pubkey" >> "$authorized_keys"
    else
        echo "[INFO] Master's public key already in authorized_keys (skip)"
    fi

    [[ ! -f "$known_hosts" ]] && touch "$known_hosts"
    chmod 600 "$known_hosts"

    echo ""
    echo "[INFO] ============================================"
    echo "[INFO] Starting to copy SSH key to worker nodes"
    echo "[INFO] ============================================"
    echo ""

    local worker_count=0
    local success_count=0
    local failed_workers=()

    for node_info in "${CLUSTER_NODES_REF[@]}"; do
        local node_ip=$(echo "$node_info" | awk '{print $1}')
        local node_name=$(echo "$node_info" | awk '{print $2}')

        if [[ "$node_name" == "master" ]]; then
            continue
        fi

        ((worker_count++))

        echo "[INFO] ----------------------------------------"
        echo "[INFO] Processing: $node_name ($node_ip)"
        echo "[INFO] ----------------------------------------"

        if ! ping -c 1 -W 2 "$node_ip" >/dev/null 2>&1; then
            echo "[WARN] Cannot reach $node_name ($node_ip) - skipping"
            failed_workers+=("$node_name ($node_ip) - unreachable")
            echo ""
            continue
        fi

        echo "[INFO] Node is reachable"

        if ! timeout 3 bash -c "cat < /dev/null > /dev/tcp/${node_ip}/22" 2>/dev/null; then
            echo "[WARN] SSH port (22) is not open on $node_name ($node_ip) - skipping"
            failed_workers+=("$node_name ($node_ip) - SSH port closed")
            echo ""
            continue
        fi

        echo "[INFO] SSH port is open"

        if ! ssh-keygen -F "$node_ip" >/dev/null 2>&1; then
            echo "[INFO] Adding $node_ip to known_hosts..."
            ssh-keyscan -H -t rsa,ecdsa,ed25519 "$node_ip" 2>/dev/null >> "$known_hosts"
        fi

        if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
            "${current_user}@${node_ip}" "exit" 2>/dev/null; then
            echo "[INFO] Passwordless SSH already configured for $node_name (skip)"
            ((success_count++))
            echo ""
            continue
        fi

        echo "[INFO] Configuring passwordless SSH for $node_name"
        echo "[INFO] You will be prompted for password of ${current_user}@${node_ip}"
        echo ""

        local copy_output
        copy_output=$(mktemp)
        TEMP_FILES+=("$copy_output")

        if ssh-copy-id -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new \
            -i "$public_key" "${current_user}@${node_ip}" >"$copy_output" 2>&1; then
            echo ""

            if grep -q "Number of key(s) added:" "$copy_output" 2>/dev/null; then
                echo "[INFO] ✓ Successfully copied SSH key to $node_name"
                ((success_count++))

                if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
                    "${current_user}@${node_ip}" "echo '[INFO] SSH connection verified'" 2>/dev/null; then
                    echo "[INFO] ✓ Passwordless SSH verified for $node_name"
                else
                    echo "[WARN] SSH key copied but passwordless login failed for $node_name"
                    echo "[WARN] This might indicate permission issues on remote authorized_keys"
                fi
            else
                echo "[INFO] ✓ SSH key was already present on $node_name"
                ((success_count++))
            fi
        else
            echo ""
            echo "[ERROR] ✗ Failed to copy SSH key to $node_name"

            # Show error details if not a permission denied error
            if ! grep -qi "permission denied" "$copy_output" 2>/dev/null; then
                local error_msg
                error_msg=$(tail -n 2 "$copy_output" | tr '\n' ' ' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
                [[ -n "$error_msg" ]] && echo "[ERROR] Details: $error_msg"
            fi

            failed_workers+=("$node_name ($node_ip) - copy failed")
        fi

        echo ""
    done

    echo ""
    echo "[INFO] ============================================"
    echo "[INFO] SSH Key Synchronization Summary"
    echo "[INFO] ============================================"
    echo "[INFO] Total workers: $worker_count"
    echo "[INFO] Successful: $success_count"
    echo "[INFO] Failed: $((worker_count - success_count))"

    if [[ ${#failed_workers[@]} -gt 0 ]]; then
        echo ""
        echo "[WARN] Failed workers:"
        for failed in "${failed_workers[@]}"; do
            echo "       - $failed"
        done
    fi

    echo ""

    if [[ $success_count -eq $worker_count ]]; then
        echo "[INFO] ✓ All workers configured successfully!"
        return 0
    elif [[ $success_count -gt 0 ]]; then
        echo "[WARN] Partial success: $success_count/$worker_count workers configured"
        return 0
    else
        echo "[ERROR] Failed to configure any workers"
        return 1
    fi
}


# =====:: system tuning for hadoop/spark

setup_system_tuning() {
    echo "[INFO] Starting system tuning for Hadoop/Spark..."

    # 1. Disable Swap
    echo "[INFO] Disabling swap..."
    sudo swapoff -a
    sudo sed -i '/swap/s/^/#/' /etc/fstab

    # 2. Configure Limits (ulimit)
    echo "[INFO] Configuring system limits (/etc/security/limits.conf)..."
    sudo tee /etc/security/limits.conf >/dev/null <<EOF
* soft nofile 65536
* hard nofile 65536
* soft nproc 32768
* hard nproc 32768
EOF

    # 3. Install and configure Chrony for time sync
    echo "[INFO] Installing and configuring Chrony for time synchronization..."
    sudo apt-get update -y && sudo apt-get install -y chrony
    sudo systemctl enable chrony
    sudo systemctl start chrony

    # 4. Configure Firewall (Open Hadoop/Spark ports or disable UFW)
    echo "[INFO] Configuring firewall (disabling UFW for internal cluster safety)..."
    sudo ufw disable || true

    # 5. Set Transparent Huge Pages (THP) to madvise or disabled (recommended for Hadoop)
    echo "[INFO] Configuring Transparent Huge Pages..."
    if [[ -f /sys/kernel/mm/transparent_hugepage/enabled ]]; then
        echo "madvise" | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null
    fi

    echo "[INFO] System tuning completed."
}

# =====: main

main() {
    . /etc/os-release
    [[ "$ID" != "ubuntu" && "$ID" != "debian" ]] && {
        echo "[ERROR] Unsupported OS: $ID"
        exit 1
    }

    require_sudo

    if ! sudo -n true 2>/dev/null; then
        echo "[ERROR] This script requires sudo privileges"
        exit 1
    fi

    local ROLE=""
    local HOSTNAME=""
    local STATIC_IP=""
    local GATEWAY_IP=""
    local ONLINE_MODE="offline"
    local SYNC_KEYS=0

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
            '-r'|'--role')
                ROLE="${2:-}"
                shift 2
                ;;
            '-n'|'--hostname')
                HOSTNAME="${2:-}"
                shift 2
                ;;
            '-i'|'--static-ip')
                STATIC_IP="${2:-}"
                shift 2
                ;;
            '-g'|'--gateway')
                GATEWAY_IP="${2:-}"
                shift 2
                ;;
            '--online')
                ONLINE_MODE="online"
                shift 1
                ;;
            '--sync')
                SYNC_KEYS=1
                shift 1
                ;;
            *)
                echo "[ERROR] Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Validation
    if [[ -n "$STATIC_IP" && -z "$GATEWAY_IP" ]]; then
        echo "[ERROR] --gateway is required when using --static-ip"
        exit 1
    fi

    [[ -z "$GATEWAY_IP" ]] && GATEWAY_IP="$GATEWAY_IP_DEFAULT"

    if [[ -n "$ROLE" && -z "$HOSTNAME" ]]; then
        echo "[ERROR] --hostname is required when --role is specified"
        exit 1
    fi

    # Validate --sync requires --role master
    if [[ $SYNC_KEYS -eq 1 ]]; then
        if [[ -z "$ROLE" ]]; then
            echo "[ERROR] --sync requires --role master"
            exit 1
        elif [[ "$ROLE" != "master" ]]; then
            echo "[ERROR] --sync can only be used with --role master"
            exit 1
        fi
    fi

    # Execute setup tasks
    local run_anything=0

    if [[ -n "$STATIC_IP" ]]; then
        echo "[INFO] Running setup_ip (mode=$ONLINE_MODE)"
        setup_ip "$STATIC_IP" "$GATEWAY_IP" "$ONLINE_MODE" || {
            echo "[ERROR] setup_ip failed"
            exit 1
        }
        run_anything=1
    fi


    if [[ -n "$ROLE" && -n "$HOSTNAME" ]]; then
        echo "[INFO] Running setup_ssh_and_hostname"
        setup_ssh_and_hostname "$ROLE" "$HOSTNAME" CLUSTER_NODES || {
            echo "[ERROR] setup_ssh_and_hostname failed"
            exit 1
        }

        # Call system tuning after hostname/ssh setup
        setup_system_tuning

        run_anything=1
    fi


    if [[ $SYNC_KEYS -eq 1 ]]; then
        echo "[INFO] Running sync_ssh_keys"
        sync_ssh_keys CLUSTER_NODES "$USER" || {
            echo "[ERROR] sync_ssh_keys failed"
            exit 1
        }
        run_anything=1
    fi

    if [[ "$run_anything" -eq 0 ]]; then
        echo "[WARN] No setup action executed"
        usage
    fi
}

main "$@"
