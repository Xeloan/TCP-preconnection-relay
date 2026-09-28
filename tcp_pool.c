#define _GNU_SOURCE
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

#define MAX_POOL_SIZE 256
#define DEFAULT_POOL_SIZE 24 //连接池大小
#define DEFAULT_REFILL_BATCH 8 //预链接池补充最大线程
#define DEFAULT_CONNECT_TIMEOUT 5
#define DEFAULT_IDLE_TIMEOUT 240 //空闲tcp(被使用过后的)回收时长
#define DEFAULT_HALF_CLOSE_TIMEOUT 10 //半关闭态最多保留10秒
#define DEFAULT_PRECONNECT_TTL_MS 50000 //预连接轮换周期

#define DEFAULT_SPLICE_CHUNK (256 * 1024) //单次数据搬运量
#define MIN_SPLICE_CHUNK     (16 * 1024)
#define MAX_SPLICE_CHUNK     (1024 * 1024)

#define UDP_TABLE_SIZE      1024
#define DEFAULT_UDP_IDLE_TIMEOUT 60 //udp单端超时时长
#define DEFAULT_UDP_SOCKET_BUFFER (4 * 1024 * 1024)
#define DEFAULT_LISTEN_BACKLOG 16384
#define EPOLL_EVENTS_MAX 1024

#define TAG_CONN_SIDE   ((uintptr_t)1)   //打tag
#define TAG_UDP_ASSOC   ((uintptr_t)2)   
#define TAG_MASK2       ((uintptr_t)3)   
#define TAG_UDP_LISTEN  ((void*)4)       

static bool LOG_ENABLE = true;     //日志开关，懒，就做了一档
#define DEFAULT_LOG_RATE_PER_SEC 24
#define LOG_QUEUE_MAX 4096

#ifndef TCP_USER_TIMEOUT
#define TCP_USER_TIMEOUT 18
#endif

static int cfg_pool_size = DEFAULT_POOL_SIZE;
static int cfg_refill_batch = DEFAULT_REFILL_BATCH;
static int cfg_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
static int cfg_idle_timeout = DEFAULT_IDLE_TIMEOUT;
static int cfg_half_close_timeout = DEFAULT_HALF_CLOSE_TIMEOUT;
static int cfg_preconnect_ttl_ms = DEFAULT_PRECONNECT_TTL_MS;
static size_t cfg_splice_chunk = DEFAULT_SPLICE_CHUNK;
static int cfg_udp_idle_timeout = DEFAULT_UDP_IDLE_TIMEOUT;
static int cfg_udp_socket_buffer = DEFAULT_UDP_SOCKET_BUFFER;
static int cfg_listen_backlog = DEFAULT_LISTEN_BACKLOG;
static int cfg_log_rate_per_sec = DEFAULT_LOG_RATE_PER_SEC;
static int cfg_tcp_keepidle = 360;
static int cfg_tcp_keepintvl = 15;
static int cfg_tcp_keepcnt = 1;
static int cfg_tcp_user_timeout_ms = 0;

typedef struct LogNode {
    char *msg;
    struct LogNode *next;
} LogNode;

static pthread_mutex_t log_mtx = PTHREAD_MUTEX_INITIALIZER;
static LogNode *log_head = NULL, *log_tail = NULL;
static int log_queue_len = 0;
static int log_dropped = 0;

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
    if (log_queue_len >= LOG_QUEUE_MAX) {
        log_dropped++;
        pthread_mutex_unlock(&log_mtx);
        free(node->msg);
        free(node);
        return;
    }
    if (!log_tail) log_head = log_tail = node;
    else { log_tail->next = node; log_tail = node; }
    log_queue_len++;
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
    static int quota = DEFAULT_LOG_RATE_PER_SEC;

    uint64_t sec = now_ms / 1000ULL;
    if (sec != cur_sec) { cur_sec = sec; quota = cfg_log_rate_per_sec; }
    if (quota <= 0) {
        LogNode *nodes = NULL;
        pthread_mutex_lock(&log_mtx);
        nodes = log_head;
        log_head = log_tail = NULL;
        log_dropped += log_queue_len;
        log_queue_len = 0;
        pthread_mutex_unlock(&log_mtx);
        while (nodes) {
            LogNode *next = nodes->next;
            free(nodes->msg);
            free(nodes);
            nodes = next;
        }
        return;
    }

    while (quota > 0) {
        int dropped = 0;

        pthread_mutex_lock(&log_mtx);
        dropped = log_dropped;
        log_dropped = 0;
        pthread_mutex_unlock(&log_mtx);

        if (dropped > 0) {
            char buf[96];
            int n = snprintf(buf, sizeof(buf), "Log dropped: %d", dropped);
            if (n > 0) {
                (void)write(STDOUT_FILENO, buf, (size_t)n);
                (void)write(STDOUT_FILENO, "\n", 1);
                quota--;
                continue;
            }
        }

        LogNode *node = NULL;

        pthread_mutex_lock(&log_mtx);
        if (log_head) {
            node = log_head;
            log_head = log_head->next;
            if (!log_head) log_tail = NULL;
            log_queue_len--;
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
    pthread_mutex_lock(&log_mtx);
    int dropped = log_dropped;
    log_dropped = 0;
    pthread_mutex_unlock(&log_mtx);
    if (dropped > 0) {
        char buf[96];
        int n = snprintf(buf, sizeof(buf), "Log dropped: %d", dropped);
        if (n > 0) {
            (void)write(STDOUT_FILENO, buf, (size_t)n);
            (void)write(STDOUT_FILENO, "\n", 1);
        }
    }
    while (1) {
        LogNode *node = NULL;

        pthread_mutex_lock(&log_mtx);
        if (log_head) {
            node = log_head;
            log_head = log_head->next;
            if (!log_head) log_tail = NULL;
            log_queue_len--;
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
static int parse_env_int(const char *name, int def, int min, int max) {
    const char *val = getenv(name);
    if (!val || !*val) return def;

    errno = 0;
    char *end = NULL;
    long n = strtol(val, &end, 10);
    if (errno != 0 || end == val || *end != '\0') {
        fprintf(stderr, "WARN: invalid %s=%s, using %d\n", name, val, def);
        return def;
    }
    if (n < min) n = min;
    if (n > max) n = max;
    return (int)n;
}

static bool parse_env_bool(const char *name, bool def) {
    const char *val = getenv(name);
    if (!val || !*val) return def;
    if (strcmp(val, "0") == 0 || strcasecmp(val, "false") == 0 ||
        strcasecmp(val, "no") == 0 || strcasecmp(val, "off") == 0) {
        return false;
    }
    if (strcmp(val, "1") == 0 || strcasecmp(val, "true") == 0 ||
        strcasecmp(val, "yes") == 0 || strcasecmp(val, "on") == 0) {
        return true;
    }
    fprintf(stderr, "WARN: invalid %s=%s, using %s\n", name, val, def ? "on" : "off");
    return def;
}

static void load_runtime_config(void) {
    cfg_pool_size = parse_env_int("POOL_SIZE", DEFAULT_POOL_SIZE, 0, MAX_POOL_SIZE);
    cfg_refill_batch = parse_env_int("REFILL_BATCH", DEFAULT_REFILL_BATCH, 1, MAX_POOL_SIZE);
    if (cfg_refill_batch > cfg_pool_size && cfg_pool_size > 0) cfg_refill_batch = cfg_pool_size;
    cfg_connect_timeout = parse_env_int("CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT, 1, 120);
    cfg_idle_timeout = parse_env_int("IDLE_TIMEOUT", DEFAULT_IDLE_TIMEOUT, 30, 86400);
    cfg_half_close_timeout = parse_env_int("HALF_CLOSE_TIMEOUT", DEFAULT_HALF_CLOSE_TIMEOUT, 1, 300);
    cfg_preconnect_ttl_ms = parse_env_int("PRECONNECT_TTL_MS", DEFAULT_PRECONNECT_TTL_MS, 10000, 3600000);
    cfg_splice_chunk = (size_t)parse_env_int("SPLICE_CHUNK", DEFAULT_SPLICE_CHUNK,
                                             MIN_SPLICE_CHUNK, MAX_SPLICE_CHUNK);
    cfg_udp_idle_timeout = parse_env_int("UDP_IDLE_TIMEOUT", DEFAULT_UDP_IDLE_TIMEOUT, 5, 3600);
    cfg_udp_socket_buffer = parse_env_int("UDP_SOCKET_BUFFER", DEFAULT_UDP_SOCKET_BUFFER,
                                          64 * 1024, 64 * 1024 * 1024);
    cfg_listen_backlog = parse_env_int("LISTEN_BACKLOG", DEFAULT_LISTEN_BACKLOG, 128, 65535);
    cfg_log_rate_per_sec = parse_env_int("LOG_RATE_PER_SEC", DEFAULT_LOG_RATE_PER_SEC, 0, 10000);
    cfg_tcp_keepidle = parse_env_int("TCP_KEEPIDLE", 360, 30, 86400);
    cfg_tcp_keepintvl = parse_env_int("TCP_KEEPINTVL", 15, 1, 3600);
    cfg_tcp_keepcnt = parse_env_int("TCP_KEEPCNT", 1, 1, 30);
    cfg_tcp_user_timeout_ms = parse_env_int("TCP_USER_TIMEOUT_MS", 0, 0, 3600000);
    LOG_ENABLE = parse_env_bool("LOG_ENABLE", true);
}

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

static void set_common_socket_options(int fd) {
    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
}

static void set_tcp_socket_options(int fd) {//TCP优化参数,缓冲区按照内核的来，不另外设置
    int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    (void)setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

    (void)setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &cfg_tcp_keepidle, sizeof(cfg_tcp_keepidle));
    (void)setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &cfg_tcp_keepintvl, sizeof(cfg_tcp_keepintvl));
    (void)setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cfg_tcp_keepcnt, sizeof(cfg_tcp_keepcnt));
    if (cfg_tcp_user_timeout_ms > 0) {
        (void)setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT,
                         &cfg_tcp_user_timeout_ms, sizeof(cfg_tcp_user_timeout_ms));
    }

    (void)setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
}

static void set_udp_socket_options(int fd) {
    //UDP缓冲
    int rcv = cfg_udp_socket_buffer;
    int snd = cfg_udp_socket_buffer;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
}

static void grow_pipe_capacity(int pipefd[2]) {
#ifdef F_SETPIPE_SZ
    int target = (cfg_splice_chunk > (size_t)INT_MAX) ? INT_MAX : (int)cfg_splice_chunk;
    (void)fcntl(pipefd[0], F_SETPIPE_SZ, target);
    (void)fcntl(pipefd[1], F_SETPIPE_SZ, target);
#else
    (void)pipefd;
#endif
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
static int refill_fail_streak = 0;
static uint64_t refill_pause_until = 0;
static pthread_mutex_t pool_mtx = PTHREAD_MUTEX_INITIALIZER;

static bool pool_put_locked(int fd, uint64_t now_ms) {//建立好的连接放在连接池里备用
    if (pool_count < cfg_pool_size) {
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

    int s = socket(((struct sockaddr*)&remote_tcp_addr)->sa_family, SOCK_STREAM, 0);
    if (s < 0) goto fin;

    set_tcp_socket_options(s);
    if (set_nonblock(s) != 0) { close(s); goto fin; }

    int rc = connect(s, (struct sockaddr*)&remote_tcp_addr, remote_tcp_addrlen);
    if (rc == 0) goto success;
    if (errno != EINPROGRESS) { close(s); goto fin; }

    struct pollfd pfd = { .fd = s, .events = POLLOUT };
    if (poll(&pfd, 1, cfg_connect_timeout * 1000) > 0) {
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
    refill_fail_streak = 0;
    refill_pause_until = 0;
    bool ok = pool_put_locked(s, now);
    if (ok) log_enqueue("Preconnect +1，Current: %d/%d (Pending: %d)", pool_count, cfg_pool_size, pending_cnt);
    else log_enqueue("Preconnected Too Much, Clearing ...");

    pthread_mutex_unlock(&pool_mtx);
    return NULL;
}

fin:
    pthread_mutex_lock(&pool_mtx);
    pending_cnt--;
    refill_fail_streak++;
    if (refill_fail_streak >= cfg_refill_batch) {
        int backoff_ms = 200 * refill_fail_streak;
        if (backoff_ms > 5000) backoff_ms = 5000;
        refill_pause_until = mono_ms() + (uint64_t)backoff_ms;
    }
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
            if ((now - pool[i].birth_ms) > (uint64_t)cfg_preconnect_ttl_ms) {
                close(pool[i].fd);
                pool[i] = pool[--pool_count];
                log_enqueue("Checking: preconnect rotating");
            }
        }//预连接到期后主动断掉重新开

        int deficit = cfg_pool_size - (pool_count + pending_cnt);
        if (deficit > 0 && now >= refill_pause_until) {
            int want = (deficit > cfg_refill_batch) ? cfg_refill_batch : deficit;
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
    bool eof_l2r;             //local->remote方向收到EOF
    bool eof_r2l;             //remote->local方向收到EOF
    bool shut_wr_r;           //已对fd_r调用shutdown(SHUT_WR)
    bool shut_wr_l;           //已对fd_l调用shutdown(SHUT_WR)
    uint64_t half_close_since;//进入半关闭态的时间
    uint32_t events_l, events_r; //已注册的epoll事件，避免无条件MOD重新触发
    bool repoll_l, repoll_r;     //本轮达到搬运预算，需要继续处理可读数据
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

    uint32_t ev_l = EPOLLET;
    uint32_t ev_r = EPOLLET;

    if (c->connecting) {
        ev_r |= EPOLLOUT | EPOLLRDHUP;
    } else {
        //有积压时只等待目标可写。实际pipe容量可能远小于cfg_splice_chunk，
        //而且splice可能耗尽pipe buffer槽位，不能用字节数推断剩余容量。
        if (!c->eof_l2r && c->len_l2r == 0) ev_l |= EPOLLIN | EPOLLRDHUP;
        if (!c->eof_r2l && c->len_r2l == 0) ev_r |= EPOLLIN | EPOLLRDHUP;
        if (c->len_l2r > 0) ev_r |= EPOLLOUT;
        if (c->len_r2l > 0) ev_l |= EPOLLOUT;
    }

    struct epoll_event ev;
    //MOD会重新检查就绪状态，即使使用EPOLLET也不能每轮无条件调用。
    //只有事件集合变化，或本轮确实搬运到预算上限时，才重新注册。
    if (ev_l != c->events_l || c->repoll_l) {
        ev.events = ev_l;
        ev.data.ptr = (void*)((uintptr_t)c | (uintptr_t)0);
        int op = c->events_l ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        if (epoll_ctl(epfd, op, c->fd_l, &ev) != 0) { conn_close(c); return; }
        c->events_l = ev_l;
    }
    if (ev_r != c->events_r || c->repoll_r) {
        ev.events = ev_r;
        ev.data.ptr = (void*)((uintptr_t)c | TAG_CONN_SIDE);
        int op = c->events_r ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        if (epoll_ctl(epfd, op, c->fd_r, &ev) != 0) { conn_close(c); return; }
        c->events_r = ev_r;
    }
    c->repoll_l = c->repoll_r = false;
}

typedef enum { PUMP_OK = 0, PUMP_EOF = 1, PUMP_ERR = 2, PUMP_MORE = 3 } pump_status_t;

static pump_status_t pump(int src_fd, int dst_fd, int pipe_in, int pipe_out,
                          size_t *pipe_len, uint64_t now_ms, uint64_t *last_ts) {
    size_t moved = 0;

    for (;;) {
        //先排空pipe，再读取源端。遇到下游背压立即等待EPOLLOUT。
        while (*pipe_len > 0) {
            ssize_t n = splice(pipe_out, NULL, dst_fd, NULL, *pipe_len,
                               SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
            if (n > 0) {
                *pipe_len -= (size_t)n;
                *last_ts = now_ms;
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return PUMP_OK;
            return PUMP_ERR;
        }

        //限制单次处理量，避免高速连接独占事件线程；显式重新触发可读端，
        //不能依赖无条件MOD，否则修复空转后可能遗漏尚未读完的ET事件。
        if (moved >= cfg_splice_chunk) return PUMP_MORE;

        ssize_t n = splice(src_fd, NULL, pipe_in, NULL, cfg_splice_chunk - moved,
                           SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (n > 0) {
            *pipe_len += (size_t)n;
            moved += (size_t)n;
            *last_ts = now_ms;
            continue;
        }
        if (n == 0) return PUMP_EOF;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return PUMP_OK;
        return PUMP_ERR;
    }
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
    load_runtime_config();

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
    printf("Using config: %s:%d -> %d/%d\n",
       REMOTE_IP, REMOTE_TCP_PORT, REMOTE_TCP_PORT, REMOTE_UDP_PORT);
    printf("Runtime: pool=%d refill=%d splice=%zu backlog=%d udp_buf=%d log=%s\n",
           cfg_pool_size, cfg_refill_batch, cfg_splice_chunk, cfg_listen_backlog,
           cfg_udp_socket_buffer, LOG_ENABLE ? "on" : "off");
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

    if (cfg_pool_size > 0) {
        pthread_t t_m;
        pthread_create(&t_m, NULL, thread_maintain, NULL);
        pthread_detach(t_m);
    }

    epfd = epoll_create1(0);
    if (epfd < 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }

    int listen_fd = socket(((struct sockaddr*)&local_bind_addr)->sa_family, SOCK_STREAM, 0);//tcp监听
    if (listen_fd < 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }

    set_common_socket_options(listen_fd);
    if (set_nonblock(listen_fd) != 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }

    if (bind(listen_fd, (struct sockaddr*)&local_bind_addr, local_bind_addrlen) < 0 || listen(listen_fd, cfg_listen_backlog) < 0) {
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
    set_common_socket_options(udp_listen_fd);
    if (set_nonblock(udp_listen_fd) != 0) { log_enqueue("socket listen failed"); log_flush_all_force(); return 1; }
    if (bind(udp_listen_fd, (struct sockaddr*)&local_bind_addr, local_bind_addrlen) < 0) {
        log_enqueue("socket listen failed");
        log_flush_all_force();
        return 1;
    }

    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = TAG_UDP_LISTEN;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, udp_listen_fd, &ev) != 0) {
        log_enqueue("socket listen failed");
        log_flush_all_force();
        return 1;
    }

    struct epoll_event events[EPOLL_EVENTS_MAX];

    while (1) {
        int nfds = epoll_wait(epfd, events, EPOLL_EVENTS_MAX, 100);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            return 1;
        }
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

                    int rem = -1;
                    if (cfg_pool_size > 0) {
                        pthread_mutex_lock(&pool_mtx);
                        rem = pool_get_locked();
                        pthread_mutex_unlock(&pool_mtx);
                    }
                    bool connecting = false;
                    if (rem < 0) {//如果并发太高了，那么直接按照传统方式转发连接
                        log_enqueue("Exceeded Connections Pool, Direct Out...");

                        rem = socket(((struct sockaddr*)&remote_tcp_addr)->sa_family, SOCK_STREAM, 0);
                        if (rem < 0) { close(cli); continue; }
                        set_tcp_socket_options(rem);
                        if (set_nonblock(rem) != 0) { close(rem); close(cli); continue; }
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
                    c->half_close_since = 0;

                    if (pipe2(c->pipe_l2r, O_NONBLOCK) != 0 || pipe2(c->pipe_r2l, O_NONBLOCK) != 0) {
                        conn_close(c);
                        free(c);
                        continue;
                    }
                    grow_pipe_capacity(c->pipe_l2r);
                    grow_pipe_capacity(c->pipe_r2l);

                    conn_watch(c);
                    if (c->closed) {
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
                    int rc = getsockopt(c->fd_r, SOL_SOCKET, SO_ERROR, &err, &len);

                    if (rc != 0 || err != 0) {
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

            if (events[i].events & EPOLLERR) {
                log_enqueue("Connection Error: %s",
                            is_remote_fd ? "Remote" : "Local");
                conn_close(c);
                continue;
            }
            if (c->connecting) continue; //连接成功之前不向远端搬运数据
            //EPOLLRDHUP/EPOLLHUP不再立即关闭，留给pump处理EOF和半关闭

            pump_status_t st1 = PUMP_OK, st2 = PUMP_OK;

            if (!c->eof_l2r) {
                st1 = pump(c->fd_l, c->fd_r, c->pipe_l2r[1], c->pipe_l2r[0],
                           &c->len_l2r, now, &c->last_l2r);
                if (st1 == PUMP_ERR) {
                    log_enqueue("Connection Error: Local->Remote");
                    conn_close(c);
                    continue;
                }
                if (st1 == PUMP_EOF) {
                    c->eof_l2r = true;
                    if (!c->eof_r2l && c->half_close_since == 0) c->half_close_since = now;
                    else if (c->eof_r2l) c->half_close_since = 0;
                    log_enqueue("EOF: Local->Remote");
                }
            } else if (c->len_l2r > 0) {
                //方向已EOF但pipe中仍有残留数据，继续刷到目标
                bool drain_err = false;
                while (c->len_l2r > 0) {
                    ssize_t n = splice(c->pipe_l2r[0], NULL, c->fd_r, NULL,
                                       c->len_l2r,
                                       SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
                    if (n > 0) { c->len_l2r -= (size_t)n; c->last_l2r = now; }
                    else {
                        if (n < 0 && errno != EAGAIN) drain_err = true;
                        break;
                    }
                }
                if (drain_err) { conn_close(c); continue; }
            }

            if (c->eof_l2r && c->len_l2r == 0 && !c->shut_wr_r && c->fd_r >= 0) {
                shutdown(c->fd_r, SHUT_WR);
                c->shut_wr_r = true;
            }

            if (!c->eof_r2l) {
                st2 = pump(c->fd_r, c->fd_l, c->pipe_r2l[1], c->pipe_r2l[0],
                           &c->len_r2l, now, &c->last_r2l);
                if (st2 == PUMP_ERR) {
                    log_enqueue("Connection Error: Remote->Local");
                    conn_close(c);
                    continue;
                }
                if (st2 == PUMP_EOF) {
                    c->eof_r2l = true;
                    if (!c->eof_l2r && c->half_close_since == 0) c->half_close_since = now;
                    else if (c->eof_l2r) c->half_close_since = 0;
                    log_enqueue("EOF: Remote->Local");
                }
            } else if (c->len_r2l > 0) {
                bool drain_err = false;
                while (c->len_r2l > 0) {
                    ssize_t n = splice(c->pipe_r2l[0], NULL, c->fd_l, NULL,
                                       c->len_r2l,
                                       SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
                    if (n > 0) { c->len_r2l -= (size_t)n; c->last_r2l = now; }
                    else {
                        if (n < 0 && errno != EAGAIN) drain_err = true;
                        break;
                    }
                }
                if (drain_err) { conn_close(c); continue; }
            }

            if (c->eof_r2l && c->len_r2l == 0 && !c->shut_wr_l && c->fd_l >= 0) {
                shutdown(c->fd_l, SHUT_WR);
                c->shut_wr_l = true;
            }

            //双向都EOF且pipe已清空，完全关闭连接
            if (c->eof_l2r && c->eof_r2l && c->len_l2r == 0 && c->len_r2l == 0) {
                log_enqueue("Connection Fully Closed");
                conn_close(c);
                continue;
            }

            c->repoll_l = (st1 == PUMP_MORE);
            c->repoll_r = (st2 == PUMP_MORE);
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
                            (now - last_any > (uint64_t)cfg_idle_timeout * 1000ULL);
                bool half_close_timeout = (cur->eof_l2r != cur->eof_r2l) &&
                            cur->half_close_since > 0 &&
                            (now - cur->half_close_since > (uint64_t)cfg_half_close_timeout * 1000ULL);
                if (cur->connecting &&
                    now - cur->connect_start > (uint64_t)cfg_connect_timeout * 1000ULL) {
                    log_enqueue("Connect timeout");
                    conn_close(cur);
                }
                if (cur->closed || timeout || half_close_timeout) {
                    if (half_close_timeout && !cur->closed) {
                        log_enqueue("Half-close timeout(%ds)", cfg_half_close_timeout);
                        conn_close(cur);
                    } else if (timeout && !cur->closed) {
                        log_enqueue("Timeout(%ds): Local->Remote", cfg_idle_timeout);
                        log_enqueue("Timeout(%ds): Remote->Local", cfg_idle_timeout);
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
                    if (now - p->last_act > (uint64_t)cfg_udp_idle_timeout * 1000ULL) {
                        udp_remove(p, i, epfd);
                    }
                    p = nxt;
                }
            }
        }
    }

    return 0;
}
