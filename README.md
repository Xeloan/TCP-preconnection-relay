# TCP-preconnection-relay

高性能TCP/UDP转发器，类似于realm，gost那种。采用零拷贝转发，性能基本无瓶颈。UDP性能良好。TCP连接采用了预链接方式，让线路鸡和落地鸡长期维持一个连接池，随时取用。故而消除了握手延时（长距离转发，如日本转发美国，效果尤为明显），客观数据上表现为http延时减少。有完善的连接回收机制，避免了qos以及内存大量占用。

---

## 安装

一键安装：

```bash
curl -fsSL https://raw.githubusercontent.com/Xeloan/TCP-preconnection-relay/main/install.sh | bash
```

---

## 使用

### 1. 创建一个配置（例如 hk）

```bash
cp /etc/tcp_pool/default.conf /etc/tcp_pool/hk.conf
```

---

### 2. 编辑配置

```bash
nano /etc/tcp_pool/hk.conf
```

填写：

```
LOCAL_PORT=你服务器本地端口
REMOTE_IP=你的服务器IP
REMOTE_TCP_PORT=远端服务器TCP端口
REMOTE_UDP_PORT=远端服务器UDP端口
```

---

### 3. 启动

```bash
systemctl restart tcp-pool@hk
```

---

### 4. 开机自启（可选）

```bash
systemctl enable tcp-pool@hk
```

---

### 5. 查看状态

```bash
systemctl status tcp-pool@hk
```

如果看到：

```
Preconnect +1
```

说明已经正常工作

---

## 多开（多个转发）

可以创建多个配置：

```bash
cp /etc/tcp_pool/default.conf /etc/tcp_pool/us.conf
cp /etc/tcp_pool/default.conf /etc/tcp_pool/jp.conf
```

分别启动：

```bash
systemctl restart tcp-pool@us
systemctl restart tcp-pool@jp
```

---

## 示例

客户端连接：

```
服务器IP:31730
```

实际转发到：

```
REMOTE_IP:REMOTE_TCP_PORT
```

---

## 注意

* 记得放行端口（ufw / 安全组）
* LOCAL_PORT 注意别重复

---

## 说明

这个程序就是一个：

```
TCP/UDP 转发 + 连接预热（降低延迟）
```
