# TCP-preconnection-relay(正在更新中，请暂时不要使用)

高性能TCP/UDP转发器，类似于realm，gost那种。采用零拷贝转发，性能基本无瓶颈。UDP性能良好。TCP连接采用了预链接方式，让线路鸡和落地鸡长期维持一个连接池，随时取用。故而消除了握手延时（长距离转发，如日本转发美国，效果尤为明显，理论上也可用于内网转发如Po0），客观数据上表现为http延时减少，同时也没有单纯连接复用的种种副作用。有完善的连接回收机制，避免了qos以及内存大量占用。

---

## 安装

一键安装：

```bash
curl -fsSL https://raw.githubusercontent.com/Xeloan/TCP-preconnection-relay/main/install.sh | bash
```

## 效果示例

* 无预链接的转发（使用realm）：
<img width="2337" height="277" alt="image" src="https://github.com/user-attachments/assets/cba16059-ded2-43da-b571-0bcaff2ea70b" />

* 有预链接的转发:
<img width="2559" height="256" alt="image" src="https://github.com/user-attachments/assets/bc78e370-9072-4fb1-90fc-75d2a6304618" />

* 单线程测速：（需要调参）
<img width="2557" height="216" alt="image" src="https://github.com/user-attachments/assets/30c7c92e-c9d1-4f9d-80ee-9b41190a9d8f" />

* gomami最近被干了禁用了udp，实际上没问题，等好了我更新一下图片。

* 测试环境为上海移动，日本优化线路为gomami，美国西雅图落地为Bug Net（名字懒得改了），可见在转发路径高rtt情况下有明显的延时下降，同时单线程速率表现良好，和其它转发无异。

* 日本优化Gomami：https://www.gomami.io (贵，无aff）
* 美国西雅图落地：https://www.misaka.io (贵，无aff，商家也没开aff功能，国际互联非常优秀，但是日本到这家因为ddos，最近不太稳定）
* 美国西雅图落地：https://bug.pw?ref=Nifwr0tPxf （便宜点，日本过去延时稳定82ms，带宽现在比misaka足且稳定，所以有aff）
* 喜欢的话给我买包辣条
<img width="636" height="730" alt="image" src="https://github.com/user-attachments/assets/7a40db31-1e51-4e13-8aea-46f14f8ca6d1" />



