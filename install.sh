#!/bin/bash
set -e
echo "Installing TCP-preconnection-relay..."
apt update
apt install -y nano
apt install -y build-essential
curl -L -o /root/tcp_pool.c \
https://raw.githubusercontent.com/Xeloan/TCP-preconnection-relay/main/tcp_pool.c
gcc -O2 -pthread -march=native -o /root/tcp_pool /root/tcp_pool.c
mkdir -p /etc/tcp_pool

cat > /etc/tcp_pool/default.conf <<EOF
#REQUIRED
#本地端口，记得ufw或者服务商的防火墙打开
LOCAL_PORT=
#远端ip，你转发的目标服务器
REMOTE_IP=
#远端的接收TCP的端口
REMOTE_TCP_PORT=
#远端的接收UDP的端口（如果你的服务端UDP和TCP跑在一个端口的，填写一样就行）
REMOTE_UDP_PORT=
EOF

cat > /etc/systemd/system/tcp-pool@.service <<EOF
[Unit]
Description=High Performance TCP Connection Pool (C Version)
After=network.target xray.service

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

true

echo ""
echo "========================================"
echo " Install completed!"
echo "========================================"
echo "先复制一份配置文件，例如："
echo "cp /etc/tcp_pool/default.conf /etc/tcp_pool/hk.conf，这个hk用来区分不同转发，就是你可以写us，jp等，代表不同的落地，用于创建多个转发"
echo "用nano /etc/tcp_pool/hk.conf编辑配置文件，没有nano装一个，hk改成你上面自己写的us或者jp"
echo "然后填写好配置文件"
echo "写好了就systemctl restart tcp-pool@hk，记得hk改成你自己的"
echo "要开机自启的话就systemctl enable tcp-pool@hk，记得hk改成你自己的"
echo "然后看日志systemctl status tcp-pool@hk，记得hk改成你自己的，如果看到一坨Preconnect +1说明通了"
