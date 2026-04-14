#define _GNU_SOURCE
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

//配置
static char *LOCAL_IP;
static struct sockaddr_storage local_bind_addr;
static socklen_t local_bind_addrlen;
static struct sockaddr_storage remote_tcp_addr;
static socklen_t remote_tcp_addrlen;
static struct sockaddr_storage remote_udp_addr;
static socklen_t remote_udp_addrlen;
static int LOCAL_PORT; //本地端口，记得ufw或者服务商防火墙放开，我老是忘
static char *REMOTE_IP;
static int REMOTE_TCP_PORT; //目标服务器TCP端口
static int REMOTE_UDP_PORT; //目标服务器UDP端口

#define POOL_SIZE       24 //连接池大小
#define REFILL_BATCH    8 //预链接池补充最大线程
#define CONNECT_TIMEOUT 5
#define IDLE_TIMEOUT    240 //空闲tcp(被使用过后的)回收时长

#define SPLICE_CHUNK (64 * 1024) //单次数据搬运量

#define UDP_TABLE_SIZE      1024
#define UDP_IDLE_TIMEOUT    60 //udp单端超时时长

#define TAG_CONN_SIDE   ((uintptr_t)1)   //打tag
#define TAG_UDP_ASSOC   ((uintptr_t)2)   
#define TAG_MASK2       ((uintptr_t)3)   
#define TAG_UDP_LISTEN  ((void*)4)       

static bool LOG_ENABLE = true;     //日志开关，懒，就做了一档
#define LOG_RATE_PER_SEC 24        //每秒最多输出 24 条，多余排队

typedef struct LogNode {
    char *msg;
    struct LogNode *next;
} LogNode;

static pthread_mutex_t log_mtx = PTHREAD_MUTEX_INITIALIZER;
static LogNode *log_head = NULL, *log_tail = NULL;

static void log_enqueue_v(const char *fmt, va_list ap) {//产生日志
    if (!LOG_ENABLE) return;

    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap2);
    va_end(ap2);
    if (n <= 0) return;

    char *buf = (char*)malloc((size_t)n + 1);
    if (!buf) return;
    vsnprintf(buf, (size_t)n + 1, fmt, ap);

    LogNode *node = (LogNode*)malloc(sizeof(LogNode));
    if (!node) { free(buf); return; }
    node->msg = buf;
    node->next = NULL;

    pthread_mutex_lock(&log_mtx);
    if (!log_tail) log_head = log_tail = node;
    else { log_tail->next = node; log_tail = node; }
    pthread_mutex_unlock(&log_mtx);
}

static void log_enqueue(const char *fmt, ...) {
    if (!LOG_ENABLE) return;
    va_list ap;
    va_start(ap, fmt);
    log_enqueue_v(fmt, ap);
    va_end(ap);
}

static void log_flush_rate_limited(uint64_t now_ms) {//日志限速
    if (!LOG_ENABLE) return;

    static uint64_t cur_sec = 0;
    static int quota = LOG_RATE_PER_SEC;

    uint64_t sec = now_ms / 1000ULL;
    if (sec != cur_sec) { cur_sec = sec; quota = LOG_RATE_PER_SEC; }
    if (quota <= 0) return;

    while (quota > 0) {
        LogNode *node = NULL;

        pthread_mutex_lock(&log_mtx);
        if (log_head) {
            node = log_head;
            log_head = log_head->next;
            if (!log_head) log_tail = NULL;
        }
        pthread_mutex_unlock(&log_mtx);

        if (!node) break;

        (void)write(STDOUT_FILENO, node->msg, strlen(node->msg));
        (void)write(STDOUT_FILENO, "\n", 1);
        free(node->msg);
        free(node);
        quota--;
    }
}

static void log_flush_all_force(void) {
    if (!LOG_ENABLE) return;
    while (1) {
        LogNode *node = NULL;

        pthread_mutex_lock(&log_mtx);
        if (log_head) {
            node = log_head;
            log_head = log_head->next;
            if (!log_head) log_tail = NULL;
        }
        pthread_mutex_unlock(&log_mtx);

        if (!node) break;

        (void)write(STDOUT_FILENO, node->msg, strlen(node->msg));
        (void)write(STDOUT_FILENO, "\n", 1);
        free(node->msg);
        free(node);
    }
}

//工具函数
static int resolve_addr(const char *host, int port, int socktype,
                        struct sockaddr_storage *out, socklen_t *outlen) {
    struct addrinfo hints, *res = NULL, *rp = NULL;
    char portstr[16];

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;      // v4 / v6 都行
    hints.ai_socktype = socktype;
    hints.ai_flags = AI_NUMERICSERV | AI_PASSIVE;

    snprintf(portstr, sizeof(portstr), "%d", port);

    int rc = getaddrinfo(host, portstr, &hints, &res);
    if (rc != 0) return -1;

    for (rp = res; rp; rp = rp->ai_next) {
        if ((socklen_t)rp->ai_addrlen > sizeof(*out)) continue;
        memcpy(out, rp->ai_addr, rp->ai_addrlen);
        *outlen = (socklen_t)rp->ai_addrlen;
        freeaddrinfo(res);
        return 0;
    }

    freeaddrinfo(res);
    return -1;
}
static uint64_t mono_ms(void) {//获取单调时间
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static int set_nonblock(int fd) {//设置非阻塞
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl < 0) return -1;
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static inline void safe_close(int *fd) {//安全关闭连接
    if (*fd >= 0) { close(*fd); *fd = -1; }
}

static void set_tcp_socket_options(int fd) {//TCP优化参数,缓冲区按照内核的来，不另外设置
    int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    (void)setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

    int idle = 360, intvl = 15, cnt = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    (void)setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    (void)setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));

    (void)setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
}

static void set_udp_socket_options(int fd) {
    //UDP缓冲
    int rcv = 4 * 1024 * 1024;
    int snd = 4 * 1024 * 1024;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
}

//连接池僵尸连接探测
static bool tcp_socket_dead_fast(int fd) {
    struct pollfd pfd;
    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    pfd.events = POLLIN;

    int r = poll(&pfd, 1, 0);
    if (r <= 0) return false;

    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) return true;

    if (pfd.revents & POLLIN) {
        char c;
        ssize_t n = recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) return true;
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) return true;
    }

    int err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err != 0) return true;

    return false;
}

//预链接池
typedef struct { int fd; uint64_t birth_ms; } pool_item_t;
static pool_item_t pool[256];
static int pool_count = 0;
static int pending_cnt = 0;
static pthread_mutex_t pool_mtx = PTHREAD_MUTEX_INITIALIZER;

static bool pool_put_locked(int fd, uint64_t now_ms) {//建立好的连接放在连接池里备用
    if (pool_count < POOL_SIZE) {
        pool[pool_count].fd = fd;
        pool[pool_count].birth_ms = now_ms;
        pool_count++;
        return true;
    }
    close(fd);
    return false;
}

static int pool_get_locked(void) {//取连接
    if (pool_count > 0) return pool[--pool_count].fd;
    return -1;
}

static void *thread_refill(void *arg) {//连接线程补充
    (void)arg;
//打个标，我在这改过，防止忘了

    int s = socket(((struct sockaddr*)&remote_tcp_addr)->sa_family, SOCK_STREAM, 0);
    if (s < 0) goto fin;

    set_tcp_socket_options(s);
    if (set_nonblock(s) != 0) { close(s); goto fin; }

    int rc = connect(s, (struct sockaddr*)&remote_tcp_addr, remote_tcp_addrlen);
    if (rc == 0) goto success;
    if (errno != EINPROGRESS) { close(s); goto fin; }

    struct pollfd pfd = { .fd = s, .events = POLLOUT };
    if (poll(&pfd, 1, CONNECT_TIMEOUT * 1000) > 0) {
        int err = 0;
        socklen_t len = sizeof(err);
        getsockopt(s, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err == 0) goto success;
    }

    close(s);
    goto fin;

success: {
    uint64_t now = mono_ms();
    pthread_mutex_lock(&pool_mtx);

    pending_cnt--;
    bool ok = pool_put_locked(s, now);
    if (ok) log_enqueue("Preconnect +1，Current: %d/%d (Pending: %d)", pool_count, POOL_SIZE, pending_cnt);
    else log_enqueue("Preconnected Too Much, Clearing ...");

    pthread_mutex_unlock(&pool_mtx);
    return NULL;
}

fin:
    pthread_mutex_lock(&pool_mtx);
    pending_cnt--;
    pthread_mutex_unlock(&pool_mtx);
    return NULL;
}

static void *thread_maintain(void *arg) {//自动补充连接，以及连接每50s自动清扫一次（避免有些运营商tcp长连接持续降权）
    (void)arg;
    while (1) {
        uint64_t now = mono_ms();

        pthread_mutex_lock(&pool_mtx);

        for (int i = pool_count - 1; i >= 0; i--) {
            if (tcp_socket_dead_fast(pool[i].fd)) {
                close(pool[i].fd);
                pool[i] = pool[--pool_count];
                log_enqueue("Checking: Clear Zombies");
            }
        }

        for (int i = pool_count - 1; i >= 0; i--) {
            if ((now - pool[i].birth_ms) > 50000) {
                close(pool[i].fd);
                pool[i] = pool[--pool_count];
                log_enqueue("Checking: 50s rotating");
            }
        }//50s后没被使用的连接主动断掉重新开

        int deficit = POOL_SIZE - (pool_count + pending_cnt);
        if (deficit > 0) {
            int want = (deficit > REFILL_BATCH) ? REFILL_BATCH : deficit;
            for (int k = 0; k < want; k++) {
                pthread_t t;
                int prc = pthread_create(&t, NULL, thread_refill, NULL);
                if (prc == 0) {
                    pthread_detach(t);
                    pending_cnt++; //连接会有过程，所以连接中的也计入总连接中，防止补充补超了
                }
            }
        }

        pthread_mutex_unlock(&pool_mtx);
        usleep(50000);
    }
    return NULL;
}

//TCP转发结构
typedef struct Conn {
    int fd_l, fd_r; //客户端，远端
    int pipe_l2r[2], pipe_r2l[2];
    size_t len_l2r, len_r2l;//pipe中数据量
    uint64_t last_l2r, last_r2l;
    bool closed;
    struct Conn *next;
    bool connecting;          //是否处于connect 中
    uint64_t connect_start;   //connect开始时间
} Conn;

static int epfd;
static Conn *conn_list = NULL;

static void conn_close(Conn *c) {//关闭连接
    if (!c || c->closed) return;
    c->closed = true;

    if (c->fd_l >= 0) { epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd_l, NULL); safe_close(&c->fd_l); }
    if (c->fd_r >= 0) { epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd_r, NULL); safe_close(&c->fd_r); }

    safe_close(&c->pipe_l2r[0]); safe_close(&c->pipe_l2r[1]);
    safe_close(&c->pipe_r2l[0]); safe_close(&c->pipe_r2l[1]);
}

static void conn_watch(Conn *c) {
    if (!c || c->closed) return;

    uint32_t ev_l = EPOLLRDHUP | EPOLLET;
    uint32_t ev_r = EPOLLRDHUP | EPOLLET;

    if (c->len_l2r < SPLICE_CHUNK) ev_l |= EPOLLIN;
    if (c->len_r2l < SPLICE_CHUNK) ev_r |= EPOLLIN;
    if (c->len_l2r > 0) ev_r |= EPOLLOUT;
    if (c->len_r2l > 0) ev_l |= EPOLLOUT;

    struct epoll_event ev;
    ev.events = ev_l;
    ev.data.ptr = (void*)((uintptr_t)c | (uintptr_t)0);
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd_l, &ev) != 0) { conn_close(c); return; }

    ev.events = ev_r;
    ev.data.ptr = (void*)((uintptr_t)c | TAG_CONN_SIDE);
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd_r, &ev) != 0) { conn_close(c); return; }
}

typedef enum { PUMP_OK = 0, PUMP_EOF = 1, PUMP_ERR = 2 } pump_status_t;

static pump_status_t pump(int src_fd, int dst_fd, int pipe_in, int pipe_out,//核心转发，零拷贝
                          size_t *pipe_len, uint64_t now_ms, uint64_t *last_ts) {
    while (*pipe_len < SPLICE_CHUNK) {
        ssize_t n = splice(src_fd, NULL, pipe_in, NULL,
                           (SPLICE_CHUNK - *pipe_len),
                           SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (n > 0) {
            *pipe_len += (size_t)n;
            *last_ts = now_ms;
            if (*pipe_len >= SPLICE_CHUNK) break;
        } else if (n == 0) {
            return PUMP_EOF;
        } else {
            if (errno == EAGAIN) break;
            return PUMP_ERR;
        }
    }

    while (*pipe_len > 0) {
        ssize_t n = splice(pipe_out, NULL, dst_fd, NULL, *pipe_len,
                           SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (n > 0) {
            *pipe_len -= (size_t)n;
            *last_ts = now_ms;
        } else {
            if (errno == EAGAIN) break;
            return PUMP_ERR;
        }
    }

    return PUMP_OK;
}

//UDP转发，每客户端一个socket
typedef struct UdpAssoc {
    struct sockaddr_storage cli;
    socklen_t cli_len;
    int up_fd;
    uint64_t last_act;
    struct UdpAssoc *next;
} UdpAssoc;

static UdpAssoc *udp_tab[UDP_TABLE_SIZE];
static int udp_listen_fd = -1;

static inline uint32_t udp_hash_addr(const struct sockaddr_storage *a) {
    if (a->ss_family == AF_INET) {
        const struct sockaddr_in *v4 = (const struct sockaddr_in*)a;
        uint32_t ip = ntohl(v4->sin_addr.s_addr);
        uint32_t port = ntohs(v4->sin_port);
        uint32_t x = ip ^ (ip >> 16) ^ (port * 2654435761u);
        return x & (UDP_TABLE_SIZE - 1);
    }

    if (a->ss_family == AF_INET6) {
        const struct sockaddr_in6 *v6 = (const struct sockaddr_in6*)a;
        const uint32_t *p = (const uint32_t*)&v6->sin6_addr;
        uint32_t x = p[0] ^ p[1] ^ p[2] ^ p[3] ^ ntohs(v6->sin6_port);
        return x & (UDP_TABLE_SIZE - 1);
    }

    return 0;
}

static inline bool udp_addr_eq(const struct sockaddr_storage *a,
                               const struct sockaddr_storage *b) {
    if (a->ss_family != b->ss_family) return false;

    if (a->ss_family == AF_INET) {
        const struct sockaddr_in *x = (const struct sockaddr_in*)a;
        const struct sockaddr_in *y = (const struct sockaddr_in*)b;
        return x->sin_port == y->sin_port &&
               x->sin_addr.s_addr == y->sin_addr.s_addr;
    }

    if (a->ss_family == AF_INET6) {
        const struct sockaddr_in6 *x = (const struct sockaddr_in6*)a;
        const struct sockaddr_in6 *y = (const struct sockaddr_in6*)b;
        return x->sin6_port == y->sin6_port &&
               x->sin6_scope_id == y->sin6_scope_id &&
               memcmp(&x->sin6_addr, &y->sin6_addr, sizeof(x->sin6_addr)) == 0;
    }

    return false;
}

static UdpAssoc* udp_get_or_create(const struct sockaddr_storage *cli,
                                   socklen_t cli_len,
                                   uint64_t now_ms,
                                   int epfd_) {
    uint32_t idx = udp_hash_addr(cli);

    UdpAssoc *p = udp_tab[idx];
    while (p) {
        if (udp_addr_eq(&p->cli, cli)) { p->last_act = now_ms; return p; }
        p = p->next;
    }

    int fd = socket(((struct sockaddr*)&remote_udp_addr)->sa_family, SOCK_DGRAM, 0);
    if (fd < 0) return NULL;
    set_udp_socket_options(fd);
    if (set_nonblock(fd) != 0) { close(fd); return NULL; }
    if (connect(fd, (struct sockaddr*)&remote_udp_addr, remote_udp_addrlen) != 0) { close(fd); return NULL; }

    UdpAssoc *n = (UdpAssoc*)calloc(1, sizeof(UdpAssoc));
    if (!n) { close(fd); return NULL; }
    memcpy(&n->cli, cli, cli_len);
    n->cli_len = cli_len;
    n->up_fd = fd;
    n->last_act = now_ms;

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = (void*)((uintptr_t)n | TAG_UDP_ASSOC);
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0) { close(fd); free(n); return NULL; }

    n->next = udp_tab[idx];
    udp_tab[idx] = n;
    return n;
}

static void udp_remove(UdpAssoc *u, uint32_t idx, int epfd_) {
    if (!u) return;
    epoll_ctl(epfd_, EPOLL_CTL_DEL, u->up_fd, NULL);
    close(u->up_fd);

    UdpAssoc **pp = &udp_tab[idx];
    while (*pp) {
        if (*pp == u) { *pp = u->next; break; }
        pp = &(*pp)->next;
    }
    free(u);
}

int main() {
    char *env;
    LOCAL_IP = getenv("LOCAL_IP");
    if (!LOCAL_IP) {
        fprintf(stderr, "ERROR: LOCAL_IP not set\n");
        exit(1);
    }
    env = getenv("LOCAL_PORT");
    if (!env) {
        fprintf(stderr, "ERROR: LOCAL_PORT not set\n");
        exit(1);
    }
    LOCAL_PORT = atoi(env);
    REMOTE_IP = getenv("REMOTE_IP");
    if (!REMOTE_IP) {
        fprintf(stderr, "ERROR: REMOTE_IP not set\n");
        exit(1);
    }
    env = getenv("REMOTE_TCP_PORT");
    if (!env) {
        fprintf(stderr, "ERROR: REMOTE_TCP_PORT not set\n");
        exit(1);
    }
    REMOTE_TCP_PORT = atoi(env);
    env = getenv("REMOTE_UDP_PORT");
    if (!env) {
        fprintf(stderr, "ERROR: REMOTE_UDP_PORT not set\n");
        exit(1);
    }
    REMOTE_UDP_PORT = atoi(env);
    printf("Using config: %s:%d → %d/%d\n",
       REMOTE_IP, REMOTE_TCP_PORT, REMOTE_TCP_PORT, REMOTE_UDP_PORT);
    if (resolve_addr(LOCAL_IP, LOCAL_PORT, SOCK_STREAM, &local_bind_addr, &local_bind_addrlen) != 0) {
        fprintf(stderr, "ERROR: resolve LOCAL_IP failed: %s:%d\n", LOCAL_IP, LOCAL_PORT);
        exit(1);
    }

    if (resolve_addr(REMOTE_IP, REMOTE_TCP_PORT, SOCK_STREAM, &remote_tcp_addr, &remote_tcp_addrlen) != 0) {
        fprintf(stderr, "ERROR: resolve REMOTE_TCP failed: %s:%d\n", REMOTE_IP, REMOTE_TCP_PORT);
        exit(1);
    }

    if (resolve_addr(REMOTE_IP, REMOTE_UDP_PORT, SOCK_DGRAM, &remote_udp_addr, &remote_udp_addrlen) != 0) {
        fprintf(stderr, "ERROR: resolve REMOTE_UDP failed: %s:%d\n", REMOTE_IP, REMOTE_UDP_PORT);
        exit(1);
    }
    struct rlimit r = {65535, 65535};
    (void)setrlimit(RLIMIT_NOFILE, &r);
    signal(SIGPIPE, SIG_IGN);

    pthread_t t_m;
    pthread_create(&t_m, NULL, thread_maintain, NULL);
    pthread_detach(t_m);

    epfd = epoll_create1(0);
    if (epfd < 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }

    int listen_fd = socket(((struct sockaddr*)&local_bind_addr)->sa_family, SOCK_STREAM, 0);//tcp监听
    if (listen_fd < 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }

    int one = 1;
    (void)setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (set_nonblock(listen_fd) != 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }

//打个标

    if (bind(listen_fd, (struct sockaddr*)&local_bind_addr, local_bind_addrlen) < 0 || listen(listen_fd, 4096) < 0) {
        log_enqueue("socket listen failed");
        log_flush_all_force();
        return 1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = NULL;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) != 0) {
        log_enqueue("socket listen failed");
        log_flush_all_force();
        return 1;
    }

    udp_listen_fd = socket(((struct sockaddr*)&local_bind_addr)->sa_family, SOCK_DGRAM, 0);
    if (udp_listen_fd < 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }
    set_udp_socket_options(udp_listen_fd);
    (void)setsockopt(udp_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (set_nonblock(udp_listen_fd) != 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }
    if (bind(udp_listen_fd, (struct sockaddr*)&local_bind_addr, local_bind_addrlen) < 0) {
        log_enqueue("socket listen failed");
        log_flush_all_force();
        return 1;
    }

//打个标

    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = TAG_UDP_LISTEN;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, udp_listen_fd, &ev) != 0) {
        log_enqueue("socket listen failed");
        log_flush_all_force();
        return 1;
    }

    struct epoll_event events[256];

    while (1) {
        int nfds = epoll_wait(epfd, events, 256, 100);
        uint64_t now = mono_ms();
        log_flush_rate_limited(now);

        for (int i = 0; i < nfds; i++) {
            void *tagp = events[i].data.ptr;

            //TCP连接接入，然后递给它一个已经创建好的预链接
            if (tagp == NULL) {
                while (1) {
                    int cli = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK);
                    if (cli < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    set_tcp_socket_options(cli);

                    pthread_mutex_lock(&pool_mtx);
                    int rem = pool_get_locked();
                    pthread_mutex_unlock(&pool_mtx);
                    bool connecting = false;
                    if (rem < 0) {//如果并发太高了，那么直接按照传统方式转发连接
                        log_enqueue("Exceeded Connections Pool, Direct Out...");

//打个标

                        rem = socket(((struct sockaddr*)&remote_tcp_addr)->sa_family, SOCK_STREAM, 0);
                        if (rem < 0) { close(cli); continue; }
                        set_tcp_socket_options(rem);
                        if (set_nonblock(rem) != 0) { close(rem); close(cli); continue; }
                        //fallback这里问题调整了一下///////////////////////////////////QAQ
                        int rc = connect(rem, (struct sockaddr*)&remote_tcp_addr, remote_tcp_addrlen);
                        if (rc != 0) {
                            if (errno != EINPROGRESS) {
                                close(rem);
                                close(cli);
                                continue;
                            }
                            connecting = true;
                        }
                    }

                    Conn *c = (Conn*)calloc(1, sizeof(Conn));
                    if (!c) { close(cli); close(rem); continue; }
                    c->connecting = connecting;
                    c->connect_start = now;
                    c->fd_l = cli;
                    c->fd_r = rem;
                    c->pipe_l2r[0] = c->pipe_l2r[1] = -1;
                    c->pipe_r2l[0] = c->pipe_r2l[1] = -1;
                    c->last_l2r = c->last_r2l = now;

                    if (pipe2(c->pipe_l2r, O_NONBLOCK) != 0 || pipe2(c->pipe_r2l, O_NONBLOCK) != 0) {
                        conn_close(c);
                        free(c);
                        continue;
                    }

                    struct epoll_event ev_c;

                    uint32_t ev_l = EPOLLRDHUP | EPOLLET;
                    if (!c->connecting) ev_l |= EPOLLIN;

                    ev_c.events = ev_l;
                    ev_c.data.ptr = (void*)((uintptr_t)c | (uintptr_t)0);
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cli, &ev_c) != 0) {
                        conn_close(c);
                        free(c);
                        continue;
                    }
                    uint32_t ev_r = EPOLLRDHUP | EPOLLET;
                    if (c->connecting) ev_r |= EPOLLOUT;
                    else ev_r |= EPOLLIN;

                    ev_c.events = ev_r;
                    ev_c.data.ptr = (void*)((uintptr_t)c | TAG_CONN_SIDE);
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, rem, &ev_c) != 0) {
                        conn_close(c);
                        free(c);
                        continue;
                    }

                    c->next = conn_list;
                    conn_list = c;
                }
                continue;
            }

            //UDP listener
            if (tagp == TAG_UDP_LISTEN) {
                while (1) {
                    uint8_t buf[65535];
                    struct sockaddr_storage cli;
                    socklen_t clen = sizeof(cli);

                    ssize_t n = recvfrom(udp_listen_fd, buf, sizeof(buf), 0, (struct sockaddr*)&cli, &clen);
                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }

                    UdpAssoc *u = udp_get_or_create(&cli, clen, now, epfd);
                    if (!u) continue;

                    ssize_t s = send(u->up_fd, buf, (size_t)n, MSG_DONTWAIT);
                    (void)s; // UDP发送失败直接丢包，避免阻塞
                    u->last_act = now;
                }
                continue;
            }

            uintptr_t pv = (uintptr_t)tagp;

            //UDP assoc
            if ((pv & TAG_MASK2) == TAG_UDP_ASSOC) {
                UdpAssoc *u = (UdpAssoc*)(pv & ~TAG_MASK2);
                if (!u) continue;

                while (1) {
                    uint8_t buf[65535];
                    ssize_t n = recv(u->up_fd, buf, sizeof(buf), 0);
                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }

                    ssize_t s = sendto(udp_listen_fd, buf, (size_t)n, MSG_DONTWAIT,
                                       (struct sockaddr*)&u->cli, u->cli_len);
                    (void)s;
                    u->last_act = now;
                }
                continue;
            }

            //TCP conn
            Conn *c = (Conn*)(pv & ~TAG_CONN_SIDE);
            if (!c || c->closed) continue;
            bool is_remote_fd = (pv & TAG_CONN_SIDE);
            if (is_remote_fd && c->connecting) {
                if (events[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {//这里保险还是判断完整一点
                    int err = 0;
                    socklen_t len = sizeof(err);
                    getsockopt(c->fd_r, SOL_SOCKET, SO_ERROR, &err, &len);

                    if (err != 0) {
                        log_enqueue("Connect failed");
                        conn_close(c);
                        continue;
                    }

                    // connect 成功
                    c->connecting = false;

                    conn_watch(c); // 切换为正常读写监听
                }
                continue;
            }

            if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                if (events[i].events & EPOLLERR) {
                    log_enqueue("Connection Closed Accidentally: %s",
                                is_remote_fd ? "Remote->Local" : "Local->Remote");
                } else {
                    log_enqueue("Connection Closed: %s",
                                is_remote_fd ? "Remote->Local" : "Local->Remote");
                }
                conn_close(c);
                continue;
            }

            pump_status_t st1 = pump(c->fd_l, c->fd_r, c->pipe_l2r[1], c->pipe_l2r[0],
                                     &c->len_l2r, now, &c->last_l2r);
            if (st1 == PUMP_EOF) { log_enqueue("Connection Closed: Local->Remote"); conn_close(c); continue; }
            if (st1 == PUMP_ERR) { log_enqueue("Connection Closed Accidentally: Local->Remote"); conn_close(c); continue; }

            pump_status_t st2 = pump(c->fd_r, c->fd_l, c->pipe_r2l[1], c->pipe_r2l[0],
                                     &c->len_r2l, now, &c->last_r2l);
            if (st2 == PUMP_EOF) { log_enqueue("Connection Closed: Remote->Local"); conn_close(c); continue; }
            if (st2 == PUMP_ERR) { log_enqueue("Connection Closed Accidentally: Remote->Local"); conn_close(c); continue; }

            conn_watch(c);
        }

        //每秒清理僵尸连接和半连接
        static uint64_t last_clean = 0;
        if (now - last_clean > 1000) {
            last_clean = now;

            Conn *prev = NULL, *cur = conn_list;
            while (cur) {
                Conn *next = cur->next;

                uint64_t last_any = (cur->last_l2r > cur->last_r2l) ? cur->last_l2r : cur->last_r2l;
                bool timeout = (!cur->connecting) &&
                            (now - last_any > (uint64_t)IDLE_TIMEOUT * 1000ULL);
                if (cur->connecting &&
                    now - cur->connect_start > (uint64_t)CONNECT_TIMEOUT * 1000ULL) {
                    log_enqueue("Connect timeout");
                    conn_close(cur);
                }
                if (cur->closed || timeout) {
                    if (timeout && !cur->closed) {
                        log_enqueue("Timeout(%ds): Local->Remote", IDLE_TIMEOUT);
                        log_enqueue("Timeout(%ds): Remote->Local", IDLE_TIMEOUT);
                        conn_close(cur);
                    } else if (!cur->closed) {
                        conn_close(cur);
                    }

                    if (prev) prev->next = next;
                    else conn_list = next;

                    free(cur);
                    cur = next;
                    continue;
                }

                prev = cur;
                cur = next;
            }

            for (uint32_t i = 0; i < UDP_TABLE_SIZE; i++) {
                UdpAssoc *p = udp_tab[i];
                while (p) {
                    UdpAssoc *nxt = p->next;
                    if (now - p->last_act > (uint64_t)UDP_IDLE_TIMEOUT * 1000ULL) {
                        udp_remove(p, i, epfd);
                    }
                    p = nxt;
                }
            }
        }
    }

    return 0;
}
