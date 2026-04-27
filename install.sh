#!/bin/bash
set -e

echo "正在安装 TCP-preconnection-relay v1.5..."
echo "如果报错有个括号啥的，请重新到github上复制脚本链接，有变动"

apt update
apt install -y nano curl build-essential

curl -L -o /root/tcp_pool.c \
https://raw.githubusercontent.com/Xeloan/TCP-preconnection-relay/main/tcp_pool.c

if gcc -O2 -pthread -march=native -o /root/tcp_pool /root/tcp_pool.c; then
    echo "Compile Succeeded"
else
    echo "你的服务商太抠了，给你用这么一个古董CPU......"
    gcc -O2 -pthread -march=x86-64 -mtune=generic -o /root/tcp_pool /root/tcp_pool.c
fi

mkdir -p /etc/tcp_pool

cleanup_old_tcp_pool() {
    echo "正在清空旧版配置和服务..."

    mapfile -t units < <(
        {
            systemctl list-units --full --all --no-legend 'tcp-pool@*.service' 2>/dev/null | awk '{print $1}'
            systemctl list-unit-files --full --no-legend 'tcp-pool@*.service' 2>/dev/null | awk '{print $1}'
        } | sort -u
    )

    for unit in "${units[@]}"; do
        [ -n "$unit" ] || continue
        systemctl stop "$unit" 2>/dev/null || true
        systemctl disable "$unit" 2>/dev/null || true
    done

    rm -f /etc/tcp_pool/*.conf
    rm -f /etc/systemd/system/tcp-pool@.service

    systemctl daemon-reload || true
}

apply_tcp_tuning_generic() {
    echo "正在写入 TCP 调优（通用版）配置..."

    cat > /etc/sysctl.d/99-custom-network-tuning.conf <<'EOF'
# 通用版 TCP 调优
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc = fq
net.ipv4.tcp_fastopen = 3

net.core.somaxconn = 16384
net.ipv4.tcp_max_syn_backlog = 8192
net.core.netdev_max_backlog = 65536

net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_mtu_probing = 1

net.core.rmem_max = 33554432
net.core.wmem_max = 33554432
net.ipv4.tcp_rmem = 8192 524288 33554432
net.ipv4.tcp_wmem = 8192 524288 33554432

net.ipv4.tcp_retries2 = 12
net.ipv4.tcp_timestamps = 1
net.ipv4.tcp_sack = 1
net.ipv4.tcp_syncookies = 1
net.ipv4.ip_local_port_range = 1024 65535

net.ipv4.tcp_keepalive_time = 360
net.ipv4.tcp_keepalive_intvl = 15
net.ipv4.tcp_keepalive_probes = 1
EOF
    
    if sysctl --system; then
        echo "TCP 调优（通用版）已应用。"
    else
        echo "TCP 调优配置文件已写入，但应用时出现报错。"
        echo "你可以手动检查 sysctl 输出，或稍后执行：sysctl --system"
    fi
}

read -r -p "你是否要清空旧版配置？如果是v1.3之前版本的必须清空，因为配置大改了。 [y/N]: " CLEAR_OLD
case "$CLEAR_OLD" in
    y|Y)
        cleanup_old_tcp_pool
        ;;
    *)
        true
        ;;
esac

if [ ! -f /etc/tcp_pool/relays.conf ]; then
cat > /etc/tcp_pool/relays.conf <<'EOF'
#注意注释不能打在行尾，会解析失败，亲身踩坑
#[US] 转发标识，中括号内填写标签，比如US,HK1,HK2
#LOCAL_IP= 本地ip，如果监听v4网卡就填写0.0.0.0。如果是v6则为俩英文冒号::。只监听本机某个特定网卡ip就填那个ip就行，比如127.0.0.0，38.175.100.122。注意俩::可能会同时监听这个端口的v4。
#LOCAL_PORT= 本地端口，记得ufw或者服务商的防火墙打开
#REMOTE_IP= 远端ip，你转发的目标服务器，现在支持v6和域名
#REMOTE_TCP_PORT= 远端的接收TCP的端口
#REMOTE_UDP_PORT= 远端的接收UDP的端口（如果你的服务端UDP和TCP跑在一个端口的，填写一样就行）

#样例，看懂了删掉就行(ctrl k 快速一行行清除，小小白白可能不知道)。现在支持单文件多配置，格式就是标签加上后面一坨东西。
[JP]
LOCAL_IP=0.0.0.0
LOCAL_PORT=11451
REMOTE_IP=38.125.91.68
REMOTE_TCP_PORT=8888
REMOTE_UDP_PORT=9999

[HK]
LOCAL_IP=::
LOCAL_PORT=11451
REMOTE_IP=域名.com
REMOTE_TCP_PORT=8888
REMOTE_UDP_PORT=9999
EOF
fi

cat > /usr/local/bin/tcp-pool-parse <<'EOF'
#!/bin/bash
set -euo pipefail

SRC="/etc/tcp_pool/relays.conf"
DST="/etc/tcp_pool"

[ -f "$SRC" ] || { echo "缺少 $SRC"; exit 1; }

mkdir -p "$DST"

find "$DST" -maxdepth 1 -type f -name '*.conf' ! -name 'relays.conf' -delete

current=""
declare -A section_seen
declare -A kv

trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

reset_kv() {
    kv=(
        [LOCAL_IP]=""
        [LOCAL_PORT]=""
        [REMOTE_IP]=""
        [REMOTE_TCP_PORT]=""
        [REMOTE_UDP_PORT]=""
    )
}

is_valid_port() {
    local p="$1"
    [[ "$p" =~ ^[0-9]+$ ]] || return 1
    (( p >= 1 && p <= 65535 )) || return 1
    return 0
}

validate_and_write_section() {
    [[ -n "$current" ]] || return 0

    local key
    for key in LOCAL_IP LOCAL_PORT REMOTE_IP REMOTE_TCP_PORT REMOTE_UDP_PORT; do
        if [[ -z "${kv[$key]}" ]]; then
            echo "[$current] 缺少: $key" >&2
            exit 1
        fi
    done

    is_valid_port "${kv[LOCAL_PORT]}" || {
        echo "[$current] 不合法 LOCAL_PORT: ${kv[LOCAL_PORT]}" >&2
        exit 1
    }
    is_valid_port "${kv[REMOTE_TCP_PORT]}" || {
        echo "[$current] 不合法 REMOTE_TCP_PORT: ${kv[REMOTE_TCP_PORT]}" >&2
        exit 1
    }
    is_valid_port "${kv[REMOTE_UDP_PORT]}" || {
        echo "[$current] 不合法 REMOTE_UDP_PORT: ${kv[REMOTE_UDP_PORT]}" >&2
        exit 1
    }

    local outfile="$DST/$current.conf"
    : > "$outfile"
    chmod 600 "$outfile"

    {
        printf 'LOCAL_IP=%s\n' "${kv[LOCAL_IP]}"
        printf 'LOCAL_PORT=%s\n' "${kv[LOCAL_PORT]}"
        printf 'REMOTE_IP=%s\n' "${kv[REMOTE_IP]}"
        printf 'REMOTE_TCP_PORT=%s\n' "${kv[REMOTE_TCP_PORT]}"
        printf 'REMOTE_UDP_PORT=%s\n' "${kv[REMOTE_UDP_PORT]}"
    } > "$outfile"
}

reset_kv

while IFS= read -r raw || [ -n "$raw" ]; do
    line="${raw%$'\r'}"
    line="$(trim "$line")"

    [[ -z "$line" ]] && continue
    [[ "$line" == \#* ]] && continue
    [[ "$line" == \;* ]] && continue

    if [[ "$line" =~ ^\[(.+)\]$ ]]; then
        tag="${BASH_REMATCH[1]}"
    
        if [[ ! "$tag" =~ ^[A-Za-z0-9_-]+$ ]]; then
            echo "标签不合法（只能包含字母数字下划线横杠，参考python变量格式）: [$tag]" >&2
            exit 1
        fi
    
        next_section="$tag"
    
        validate_and_write_section
    
        current="$next_section"
        if [[ -n "${section_seen[$current]:-}" ]]; then
            echo "你标签写重复了: [$current]" >&2
            exit 1
        fi
        section_seen["$current"]=1
        reset_kv
        continue
    fi

    if [[ -z "$current" ]]; then
        echo "你漏写标签了: $line" >&2
        exit 1
    fi

    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
        key="${BASH_REMATCH[1]}"
        val="$(trim "${BASH_REMATCH[2]}")"
    else
        echo "配置项不合法 [$current]: $line" >&2
        exit 1
    fi

    case "$key" in
        LOCAL_IP|LOCAL_PORT|REMOTE_IP|REMOTE_TCP_PORT|REMOTE_UDP_PORT)
            kv["$key"]="$val"
            ;;
        *)
            echo "[$current] 你写了个莫名奇妙的配置进来: $key" >&2
            exit 1
            ;;
    esac
done < "$SRC"

validate_and_write_section

echo "配置解析完成"
EOF

chmod +x /usr/local/bin/tcp-pool-parse

cat > /etc/systemd/system/tcp-pool@.service <<'EOF'
[Unit]
Description=High Performance TCP Connection Pool (C Version)
Wants=network-online.target
After=network-online.target xray.service

[Service]
ExecStart=/root/tcp_pool
EnvironmentFile=/etc/tcp_pool/%i.conf

Nice=-10
LimitNOFILE=65535

Restart=always
RestartSec=3
User=root
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

cat > /usr/local/bin/tcp-pool-start <<'EOF'
#!/bin/bash
set -euo pipefail
tcp-pool-parse

mapfile -t old_units < <(
    {
        systemctl list-units --full --all --no-legend 'tcp-pool@*.service' 2>/dev/null | awk '{print $1}'
        systemctl list-unit-files --full --no-legend 'tcp-pool@*.service' 2>/dev/null | awk '{print $1}'
    } | sort -u
)

for unit in "${old_units[@]}"; do
    [ -n "$unit" ] || continue
    systemctl stop "$unit" 2>/dev/null || true
    systemctl disable "$unit" 2>/dev/null || true
done

shopt -s nullglob
confs=(/etc/tcp_pool/*.conf)

instances=()
for conf in "${confs[@]}"; do
    name="$(basename "$conf")"
    [[ "$name" == "relays.conf" ]] && continue
    [[ "$name" != *.conf ]] && continue
    instances+=("${name%.conf}")
done

if [ "${#instances[@]}" -eq 0 ]; then
    echo "没有可启动的转发实例，请检查 /etc/tcp_pool/relays.conf"
    exit 1
fi

for inst in "${instances[@]}"; do
    echo "正在启动并设置开机自启 tcp-pool@$inst ..."
    systemctl enable "tcp-pool@$inst"
    systemctl restart "tcp-pool@$inst"
done

echo ""
echo "全部实例已启用完成。"
echo "查看日志命令："
for inst in "${instances[@]}"; do
    echo "journalctl -u tcp-pool@$inst -f"
done
EOF

chmod +x /usr/local/bin/tcp-pool-start

echo ""
echo "========================================"
echo " Install completed!"
echo "========================================"

read -r -p "是否现在修改配置文件？ [Y/n]: " EDIT_NOW
case "$EDIT_NOW" in
    n|N)
        echo "之后可以输入以下命令修改配置文件，记得存一下哦："
        echo "nano /etc/tcp_pool/relays.conf"
        echo "之后如果要启动/重启全部转发，可以输入以下命令："
        echo "tcp-pool-start"
        ;;
    *)
        nano /etc/tcp_pool/relays.conf

        read -r -p "是否现在应用配置并启动全部转发？ [Y/n]: " START_NOW
        case "$START_NOW" in
            n|N)
                echo "之后如果要启动/重启全部转发，可以输入以下命令："
                echo "tcp-pool-start"
                ;;
            *)
                tcp-pool-start
                ;;
        esac
        ;;
esac
echo ""
read -r -p "是否进行 TCP 调优（通用版）？ [y/N]: " TCP_TUNE_NOW
case "$TCP_TUNE_NOW" in
    y|Y)
        apply_tcp_tuning_generic
        ;;
    *)
        echo "已跳过 TCP 调优。之后如需手动启用，可自行写入 /etc/sysctl.d/99-custom-network-tuning.conf 并执行 sysctl --system"
        ;;
esac
echo ""
echo "========================================"
echo " 常用命令说明"
echo "========================================"
echo "修改配置文件："
echo "nano /etc/tcp_pool/relays.conf"
echo ""
echo "应用配置并启动/重启全部转发："
echo "tcp-pool-start"
echo ""
echo "停止某个实例（把 HK 改成你自己的标签）："
echo "systemctl stop tcp-pool@HK"
echo ""
echo "禁用某个实例开机自启（把 HK 改成你自己的标签）："
echo "systemctl disable tcp-pool@HK"
echo ""
echo "查看某个实例日志（把 HK 改成你自己的标签），如果看到一坨Preconnect +1，说明成了："
echo "journalctl -u tcp-pool@HK -f"
echo "========================================"
