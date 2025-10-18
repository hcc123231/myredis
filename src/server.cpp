#include <fcntl.h>
#include <cstdint>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <string>
#include <vector>
#include <cstdio>
#include<iostream>
#include <unistd.h>
#include <sys/timerfd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unordered_map>
#include <netinet/tcp.h>
#include <time.h>
#include <sys/uio.h>
#include <string_view>
#include<csignal>
#include <charconv>
#include "../include/rdb.h"
#include "../include/resp.h"
#include "../include/kv.h"
#include "../include/server.h"
#include "../include/aof.h"
#include"../include/replica_client.h"
// namespace myredis这样是正常的命名空间，对外文件public
namespace myredis
{
    KeyValueStore gStore;
    static AofLogger gAof;
    static Rdb gRdb;
    int64_t gRepliBacklogOffset = 0;
    static std::vector<std::vector<std::string>> gReplQueue;
    // 而namespace{}这种就是匿名命名空间，对外文件来说是private的
    namespace
    {
        // 设置网络套接字为非阻塞模式
        int setNonBlock(int fd)
        {
            int flags = fcntl(fd, F_GETFL, 0); // 先获取到fd的所有状态，这个状态是掩码形式，返回状态的int形式
            if (flags == -1)
                return -1; // flags等于-1标识状态获取失败
            if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
                return -1;
            return 0;
        }
        // 将fd添加到对应的epoll中去
        int addEpoll(int epfd, int fd, uint32_t events)
        {
            epoll_event event{};
            event.events = events;
            event.data.fd = fd;
            return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event);
        }
        // 更改epoll中的某个fd的事件
        int modEpoll(int epfd, int fd, uint32_t events)
        {
            epoll_event event{};
            event.events = events;
            event.data.fd = fd;
            return epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &event);
        }
        // 封装网络连接结构体
        struct NetConnection
        {
            int _fd = -1;
            std::string _in = "";                // 接收缓冲区
            std::vector<std::string> _outChunks; // 发送块队列缓冲区
            size_t _outIndex;                    // 当前发送块的下标
            size_t _outOffset;                   // 当前发送快的块内偏移
            RespParser _parser{};                // resp解析器对象
            bool isReplica = false;              // 标志该条连接是否为从节点
        };
    }

    Server::Server(const ServerConfig &config) : _config{config} {}
    Server::~Server()
    {
        if (_listenFd >= 0)
            close(_listenFd);
        if (_epollFd >= 0)
            close(_epollFd);
    }
    int Server::setupListen()
    {
        _listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (_listenFd == -1)
        {
            std::perror("failed to alloc listen fd\n");
            return -1;
        }
        int opt = 1;
        // 设置ip,端口复用
        if (setsockopt(_listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1)
        {
            std::perror("failed to set sockopt for listen fd\n");
            return -1;
        }
        // 填充地址
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(_config._port);
        if (inet_pton(AF_INET, _config._bindAddr.c_str(), &addr.sin_addr) <= 0)
        {
            std::perror("inet_pton is failed\n");
            return -1;
        }
        // 绑定fd
        if (bind(_listenFd, (sockaddr *)&addr, sizeof(addr)) < 0)
        {
            std::perror("failed to bind\n");
            return -1;
        }
        // 设置非阻塞,listenfd的阻塞与非阻塞是相对于内核中的连接队列的这个队列来说的
        // listenfd的阻塞与否在用户态上只体现在accept这一步上，而accept函数就是从内核的连接已完成队列中拿一条连接
        // 所以如果是阻塞模式，当内核的这个连接队列中没有连接时会阻塞等待，而如果是非阻塞模式则直接返回一个错误值
        if (setNonBlock(_listenFd) == -1)
        {
            std::perror("failed to set nonblock\n");
            return -1;
        }
        // 监听listenfd
        if (listen(_listenFd, 512) == -1)
        {
            std::perror("failed to set listen\n");
            return -1;
        }
        return 0;
    }
    // 首先创建出epollfd,然后将listenfd和timerfd注册到epoll中
    int Server::setupEpoll()
    {
        _epollFd = epoll_create(1);
        if (_epollFd < 0)
        {
            std::perror("epoll fd is failed to allocate\n");
            return -1;
        }
        // listenfd设置为边缘触发，那么就是说从条件不满足到条件满足才会触发一次，提高效率
        // 这样可以配合后续的循环accept
        if (addEpoll(_epollFd, _listenFd, EPOLLIN | EPOLLET) == -1)
        {
            std::perror("failed to add listen fd to epoll\n");
            return -1;
        }
        _timerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (_timerFd < 0)
        {
            std::perror("failed to create timerfd\n");
            return -1;
        }
        itimerspec iti{};
        // iti.it_interval指首次超时时间
        iti.it_interval.tv_sec = 0;
        iti.it_interval.tv_nsec = 200 * 1000 * 1000; // 200ms
        // iti.it_value指之后超时周期
        iti.it_value = iti.it_interval;
        if (timerfd_settime(_timerFd, 0, &iti, nullptr) < 0)
        {
            std::perror("failed to timerfd_settime\n");
            return -1;
        }
        /*if (addEpoll(_epollFd, _timerFd, EPOLLIN | EPOLLET) == -1)
        {
            std::perror("failed to add timerfd to epoll\n");
            return -1;
        }*/
        return 0;
    }
    // 判断conn的outChunks是否发送完毕，如果完全发送完了，返回false，如果还有数据等待发送，返回true,这里的发送单纯是用户态数据拷贝到内核态的发送缓冲区。
    //所以如果还有数据没有发送，那么说明conn有数据可写，这样才需要epoll去检测conn的写事件，如果说conn没有用户态数据需要写，那么也就没有检测EPOLLOUT了
    static inline bool hasPending(const NetConnection &conn)
    {
        return conn._outIndex < conn._outChunks.size() || (conn._outIndex == conn._outChunks.size() && conn._outOffset != 0);
    }

    // 将conn中的_outChunk待发送数据块一次性发送出去
    static void tryFlushNow(int fd, NetConnection &conn, uint32_t &ev)
    {
        while (hasPending(conn))
        {
            const size_t maxIov = 64; // I/O vector最大的长度，简单来说就是io向量个数
            struct iovec iov[maxIov]; // struct iovec结构体其实是很简单的一个结构，第一个成员是指向内存起始地址的指针，第二个成员是这块内存的长度
            int iovIdx = 0;           // iov数组的下标
            size_t idx = conn._outIndex;
            size_t offset = conn._outOffset;
            while (idx < conn._outChunks.size() && iovIdx < (int)maxIov)
            {
                const std::string &s = conn._outChunks[idx];
                const char *base = s.data();
                size_t len = s.size();
                // 如果当前块偏移刚好到块末尾或者超出块末尾，说明当前块发送完毕，直接跳转到下一个块
                if (offset >= len)
                {
                    idx++;
                    offset = 0;
                    continue;
                }
                // 否则，说明当前块偏移在当前块的中间部分或者开头，那么意味着当前块还没有完全发送，需要提取出未发送部分到iov中
                iov[iovIdx].iov_base = (void *)(base + offset);
                iov[iovIdx].iov_len = len - offset;
                iovIdx++;
                idx++;
                offset = 0;
            }
            // 说明iov数组没有数据直接跳出循环，没有发送的需求
            if (iovIdx == 0)
                break;
            // 写数据到内核缓冲区中
            ssize_t dataLen = ::writev(conn._fd, iov, iovIdx);
            // ssize_t是有符号整数类型，但是系统库作为返回值通常只会返回io字节数或者失败-1，其他的都没有意义
            if (dataLen > 0)
            {
                size_t len = (size_t)dataLen;
                while (len > 0 && conn._outIndex < conn._outChunks.size())
                {
                    std::string &str = conn._outChunks[conn._outIndex];
                    size_t avail = str.size() - conn._outOffset;
                    if (len < avail)
                    {
                        conn._outOffset += len;
                        len = 0;
                    }
                    else if (len >= avail)
                    {
                        len -= avail;
                        conn._outOffset = 0;
                        conn._outIndex++;
                    }
                }
            }
            else if (dataLen < 0 && (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK))
            {
                break;
            }
            else
            {
                std::perror("writev");
                ev |= EPOLLRDHUP;
                break;
            }
        }
    }
    //所有的更改操作的原始命令都会记录到aof的缓存中，并且更改操作的vector记录形式会记录在grepliQueue中
    static std::string handleCommand(const RespValue &respV, const std::string *raw,const ServerConfig& config)
    {
        // 这个命令必须是array存储，因为命令往往是多个指令
        if (respV._type != RespType::Array || respV._array.empty())
            return respError("ERROR protocol");
        const RespValue &head = respV._array[0];
        // 判断数组第一个元素是不是字符串类型
        if (head._type != RespType::BulkString && head._type != RespType::SimpleString)
            return respError("ERROR command type");
        // 将命令转为大写
        std::string cmd{};
        cmd.reserve(head._bulk.size());
        for (const auto &c : head._bulk)
        {
            cmd.push_back(static_cast<char>(::toupper(c)));
        }
        if (cmd == "PING")
        {
            if (respV._array.size() <= 1)
                return respSimpleString("PONG");
            if (respV._array.size() == 2 && respV._array[1]._type == RespType::BulkString)
                return respBulkString(respV._array[1]._bulk);
            return respError("error with wrong numbers of args of command 'PING'");
        }
        if (cmd == "ECHO")
        {
            if (respV._array.size() == 2 && respV._array[1]._type == RespType::BulkString)
                return respBulkString(respV._array[1]._bulk);
            return respError("error with wrong numbers of args of command 'ECHO'");
        }
        // 字符对象的写入命令
        if (cmd == "SET")
        {
            // 检查参数个数和参数类型
            if (respV._array.size() < 3)
                return respError("error with  wrong numbers of args of command 'SET'");
            if (respV._array[1]._type != RespType::BulkString || respV._array[2]._type != RespType::BulkString)
                return respError("error with wrong type of command 'SET'");

            std::optional<int64_t> ttlMs;
            size_t i = 3;
            // 从第四个参数起开始循环检查后面的参数
            // set命令还有可能长这样:SET key value [EX seconds] [PX milliseconds],这里的EX组合和PX组合的顺序是任意的
            while (i < respV._array.size())
            {
                if (respV._array[i]._type != RespType::BulkString)
                    return respError("error with wrong type of SET");
                std::string opt;
                opt.reserve(respV._array[i]._bulk.size());
                for (const auto &c : respV._array[i]._bulk)
                    opt.push_back(static_cast<char>(::toupper(c)));
                // 秒级过期时间
                if (opt == "EX")
                {
                    if (i + 1 >= respV._array.size() || respV._array[i]._type != RespType::BulkString)
                        return respError("error with EX arg type");
                    try
                    {
                        int64_t sec = std::stoll(respV._array[i + 1]._bulk);
                        if (sec < 0)
                            return respError("error with EX arg of value");
                        ttlMs = 1000 * sec;
                    }
                    catch (...)
                    {
                        return respError("EX arg's value is not reasonable");
                    }
                    i += 2;
                    continue;
                }
                else if (opt == "PX")
                {
                    if (i + 1 >= respV._array.size() || respV._array[i]._type != RespType::BulkString)
                        return respError("error with PX arg type");
                    try
                    {
                        int64_t ms = std::stoll(respV._array[i + 1]._bulk);
                        if (ms < 0)
                            return respError("error with PX arg of value");
                        ttlMs = ms;
                    }
                    catch (...)
                    {
                        return respError("PX arg's value is not reasonable");
                    }
                    i += 2;
                    continue;
                }
                else
                    return respError("error with set option args");
            }
            // 解析完命令就可以执行命令了
            gStore.set(respV._array[1]._bulk, respV._array[2]._bulk, ttlMs);
            // 然后将这个命令按照resp协议格式使用aof持久化
            if (raw)
                gAof.appendRaw(*raw);
            else
            {
                std::vector<std::string> command;
                command.reserve(respV._array.size());
                for (const auto &s : respV._array)
                    command.push_back(s._bulk);
                gAof.appendCommand(command);
            }
            // 这里将命令存储到主从复制的增量复制队列中
            {
                std::vector<std::string> command;
                command.reserve(respV._array.size());
                for (const auto &s : respV._array)
                    command.push_back(s._bulk);
                gReplQueue.push_back(std::move(command));
            }
            return respSimpleString("OK");
        }
        if (cmd == "GET")
        {
            if (respV._array.size() != 2)
                return respError("error with the number of args of command 'GET'");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("error with the type of args of command 'GET'");
            auto opt = gStore.get(respV._array[1]._bulk);
            if (opt.has_value())
                return respBulkString(*opt);
            else
                return respNullBulk();
        }
        if (cmd == "KEYS")
        {
            std::string pattern = "*";
            if (respV._array.size() == 2)
            {
                if (respV._array[1]._type == RespType::SimpleString || respV._array[1]._type == RespType::BulkString)
                    pattern = respV._array[1]._bulk;
                else
                    return respError("error with type of args of command 'KEYS'");
            }
            else if (respV._array.size() != 1)
                return respError("error with count of args of command 'KEYS'");
            
            // 先暂时只支持*
            if (pattern == "*")
            {
                auto keys = gStore.listKeys();
                std::string out = "*" + std::to_string(keys.size()) + "\r\n";
                for (const auto &k : keys)
                    out += respBulkString(k);
                return out;
            }
            return respError("error with the args of 'KEYS'");
        }
        if (cmd == "FLUSHALL")
        {
            // 清空redis数据库,然后再进行一次rdb持久化操作，redis数据库为空，那么相应地为了保证数据一致性，rdb文件必须也清空，这里我还没有进行rdb持久化
            if (respV._array.size() != 1)
                return respError("error with count of args of 'FLUSHALL'");
            gStore.clearAll();
            //在任何与aof或者rdb或者replica相关的操作中都要考虑相关组件是否开启
            if(config._rdb._enabled){
                std::string err;
                if(!gRdb.save(gStore,err))return respError(err);
            }
            
            // aof记录
            if (raw)
                gAof.appendRaw(*raw);
            else
                gAof.appendCommand({"FLUSHALL"});
            gReplQueue.push_back({"FLUSHALL"});
            return respSimpleString("OK");
        }
        if (cmd == "DEL")
        {
            if (respV._array.size() < 2)
                return respError("error with del");
            std::vector<std::string> keys;
            keys.reserve(respV._array.size() - 1);
            for (size_t i = 1; i < respV._array.size(); i++)
            {
                if (respV._array[i]._type != RespType::BulkString)
                    return respError("error with del");
                keys.push_back(respV._array[i]._bulk);
            }
            int removed = gStore.del(keys);
            if (removed > 0)
            {
                // 那么删除成功，这样就需要将这个删除操作记录起来
                std::vector<std::string> parts;
                parts.reserve(1 + keys.size());
                parts.emplace_back("DEL");
                for (auto &k : keys)
                    parts.emplace_back(k);
                if (raw)
                    gAof.appendRaw(*raw);
                else
                    gAof.appendCommand(parts);
                gReplQueue.push_back(parts);
            }
            return respInteger(removed);
        }
        if (cmd == "EXISTS")
        {
            if (respV._array.size() != 2)
                return respError("error with exists");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("error with exists");
            bool ret = gStore.exists(respV._array[1]._bulk);
            return respInteger((ret ? 1 : 0));
        }
        if (cmd == "EXPIRE")
        {
            if (respV._array.size() != 3)
                return respError("error with expire");
            if (respV._array[1]._type != RespType::BulkString || respV._array[2]._type != RespType::BulkString)
                return respError("error with expire");
            try
            {
                int64_t sec = std::stoll(respV._array[2]._bulk);
                bool ok = gStore.expire(respV._array[1]._bulk, sec);
                if (ok)
                {
                    if (raw)
                        gAof.appendRaw(*raw);
                    else
                        gAof.appendCommand({"EXPIRE", respV._array[1]._bulk, respV._array[2]._bulk});
                    gReplQueue.push_back({"EXPIRE", respV._array[1]._bulk, respV._array[2]._bulk});
                }
                return respInteger((ok ? 1 : 0));
            }
            catch (...)
            {
                return respError("error with expire");
            }
        }
        if (cmd == "TTL")
        {
            if (respV._array.size() != 2)
                return respError("ERR wrong number of arguments for 'TTL'");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("ERR syntax");
            int64_t ret = gStore.ttl(respV._array[1]._bulk);
            return respInteger(ret);
        }
        // HSET key field value [field value ...]这是hset命令格式
        if (cmd == "HSET")
        {
            if (respV._array.size() < 4 || (respV._array.size() >= 4 && respV._array.size() % 2))
                return respError("error with the count of args of HSET");
            std::vector<std::string> args;
            for (size_t i = 1; i < respV._array.size(); i++)
            {
                if (respV._array[i]._type != RespType::BulkString)
                    return respError("error with type of args of HSET");
                if (i > 1)
                    args.push_back(respV._array[i]._bulk);
            }
            int created = gStore.hset(respV._array[1]._bulk, args);
            std::vector<std::string> out;
            for (const auto &s : respV._array)out.push_back(s._bulk);
            if (raw)
                gAof.appendRaw(*raw);
            else gAof.appendCommand(out);
            gReplQueue.push_back(out);
            return respInteger(created);
        }
        if (cmd == "HGET")
        {
            if (respV._array.size() != 3)
                return respError("ERR wrong number of arguments for 'HGET'");
            if (respV._array[1]._type != RespType::BulkString || respV._array[2]._type != RespType::BulkString)
                return respError("ERR syntax");
            auto opt = gStore.hget(respV._array[1]._bulk, respV._array[2]._bulk);
            if (opt.has_value())
                return respBulkString(*opt);
            return respNullBulk();
        }
        if (cmd == "HDEL")
        {
            if (respV._array.size() < 3)
                return respError("ERR wrong number of arguments for 'HDEL'");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("ERR syntax");
            std::vector<std::string> fields;
            for (size_t i = 2; i < respV._array.size(); i++)
            {
                if (respV._array[i]._type != RespType::BulkString)
                    return respError("error with type of HDEL");
                fields.push_back(respV._array[i]._bulk);
            }
            int removed = gStore.hdel(respV._array[1]._bulk, fields);
            if (removed > 0)
            {
                //>0说明删除成功，所以需要记录下这个hdel命令
                std::vector<std::string> command;
                command.reserve(respV._array.size());
                for (const auto &s : respV._array)command.push_back(s._bulk);
                if (raw)
                    gAof.appendRaw(*raw);
                else gAof.appendCommand(command);
                gReplQueue.push_back(command);
            }
            return respInteger(removed);
        }
        if (cmd == "HEXISTS")
        {
            if (respV._array.size() != 3)
                return respError("ERR wrong number of arguments for 'HEXISTS'");
            if (respV._array[1]._type != RespType::BulkString || respV._array[2]._type != RespType::BulkString)
                return respError("ERR syntax");
            bool ret = gStore.hexists(respV._array[1]._bulk, respV._array[2]._bulk);
            return respInteger((ret ? 1 : 0));
        }
        if (cmd == "HGETALL")
        {
            if (respV._array.size() != 2)
                return respError("ERR wrong number of arguments for 'HGETALL'");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("ERR syntax");
            auto vec = gStore.hgetAll(respV._array[1]._bulk);
            std::string out = "*" + std::to_string(vec.size()) + "\r\n";
            for (const auto &s : vec)
                out += respBulkString(s);
            return out;
        }
        if (cmd == "HLEN")
        {
            if (respV._array.size() != 2)
                return respError("ERR wrong number of arguments for 'HLEN'");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("ERR syntax");
            int len = gStore.hlen(respV._array[1]._bulk);
            return respInteger(len);
        }
        if (cmd == "ZADD")
        {
            // zadd的命令格式是zadd key score1 member1 [score2 member2 ...]
            if (respV._array.size() < 4 || respV._array.size() % 2)
                return respError("ERR wrong number of arguments for 'ZADD'");
            for (size_t i = 1; i < respV._array.size(); i++)
            {
                if (respV._array[i]._type != RespType::BulkString)
                    return respError("ERR syntax");
            }
            std::vector<std::pair<double, std::string>> args;
            try
            {
                for (size_t i = 2; i < respV._array.size(); i += 2)
                {
                    double sc = std::stoll(respV._array[i]._bulk);
                    args.emplace_back(sc, respV._array[i + 1]._bulk);
                }
            }
            catch (...)
            {
                return respError("error with ZADD");
            }
            int added = gStore.zadd(respV._array[1]._bulk, args);
            std::vector<std::string> command;
            command.reserve(respV._array.size());
            for (const auto &s : respV._array)command.push_back(s._bulk);
            if (raw)
                gAof.appendRaw(*raw);
            else gAof.appendCommand(command);
            gReplQueue.push_back(command);
            
            return respInteger(added);
        }
        if (cmd == "ZREM")
        {
            // 移除zset中的指定元素，格式如zrem key member [member ...]
            if (respV._array.size() < 3)
                return respError("ERR wrong number of arguments for 'ZREM'");
            if (respV._array[1]._type != RespType::BulkString)
                return respError("ERR syntax");
            std::vector<std::string> members;
            for (size_t i = 1; i < respV._array.size(); i++)
            {
                if (respV._array[i]._type != RespType::BulkString)
                    return respError("ERR syntax");
                members.emplace_back(respV._array[i]._bulk);
            }
            int removed = gStore.zrem(respV._array[1]._bulk, members);
            if (removed > 0)
            {
                // 当zrem删除了数据时我们才需要记录下这条命令
                /*std::vector<std::string> command;
                command.reserve(2 + members.size());*/
                std::vector<std::string> command;
                command.reserve(respV._array.size());
                for (const auto &s : respV._array)command.push_back(s._bulk);
                if (raw)
                    gAof.appendRaw(*raw);
                else gAof.appendCommand(command);
                gReplQueue.push_back(command);
                
            }
            return respInteger(removed);
        }
        if (cmd == "ZRANGE")
        {
            // zrange key start stop [withscores]
            // 在此实现成zrange key start stop
            if (respV._array.size() != 4)
                return respError("ERR wrong number of arguments for 'ZRANGE'");
            if (respV._array[1]._type != RespType::BulkString || respV._array[2]._type != RespType::BulkString || respV._array[3]._type != RespType::BulkString)
                return respError("ERR syntax");
            try
            {
                int64_t start = std::stoll(respV._array[2]._bulk);
                int64_t stop = std::stoll(respV._array[3]._bulk);
                auto members = gStore.zrange(respV._array[1]._bulk, start, stop);
                std::string out = "*" + std::to_string(members.size()) + "\r\n";
                for (const auto &m : members)
                    out += respBulkString(m);
                return out;
            }
            catch (...)
            {
                return respError("error with args swith");
            }
        }
        if (cmd == "ZSCORE")
        {
            // zscore key member
            if (respV._array.size() != 3)
                return respError("ERR wrong number of arguments for 'ZSCORE'");
            if (respV._array[1]._type != RespType::BulkString || respV._array[2]._type != RespType::BulkString)
                return respError("ERR syntax");
            auto s = gStore.zscore(respV._array[1]._bulk, respV._array[2]._bulk);
            if (!s.has_value())
                return respNullBulk();
            return respBulkString(std::to_string(*s));
        }
        if (cmd == "BGSAVE" || cmd == "SAVE")
        {
            // 这里暂时实现成阻塞主线程模式
            if (respV._array.size() != 1)
                return respError("ERR wrong number of arguments for 'BGSAVE' or 'SAVE'");
            std::string err;
            //std::cout<<"cmd save\n";
            if (!gRdb.save(gStore, err))
                return respError(std::string{"ERR rdb save faild:"} + err);
            return respSimpleString("OK");
        }
        if (cmd == "BGREWRITEAOF")
        {
            // 重写aof文件，随着时间推移，aof文件越来越大，需要对aof文件内容进行优化
            if (respV._array.size() != 1)
                return respError("ERR wrong number of arguments for 'BGREWRITEAOF'");
            std::string err;
            if (!gAof.isEnabled())
                return respError("ERR aof disabled");
            if (!gAof.bgRewrite(gStore, err))
                return respError(std::string{"ERROR"} + err);
            return respSimpleString("OK");
        }
        if (cmd == "CONFIG")
        {
            // config get/set/resetstat 后面跟配置的key值
            if (respV._array.size() < 2)
                return respError("ERR wrong number of arguments for 'CONFIG'");
            if (respV._array[1]._type != RespType::BulkString && respV._array[1]._type != RespType::SimpleString)
                return respError("ERR syntax");
            std::string sub;
            for (auto &c : respV._array[1]._bulk)
                sub.push_back(static_cast<char>(::toupper(c)));
            if (sub == "GET")
            {
                std::string pattern = "*"; // 默认get操作的参数
                if (respV._array.size() >= 3)
                {
                    // 如果get命令的长度>=3，那么说明命令自带get参数，使用自带参数就行了
                    if (respV._array[2]._type != RespType::BulkString && respV._array[2]._type != RespType::SimpleString)
                        return respError("ERR syntax");
                    pattern = respV._array[2]._bulk;
                }
                // 判断pattern与传入参数是否相同或者pattern是否为*
                auto match = [&](const std::string &k) -> bool
                {
                    if (pattern == "*")
                        return true;
                    return pattern == k;
                };
                std::vector<std::pair<std::string, std::string>> kvs;
                kvs.emplace_back("appendonly", gAof.isEnabled() ? "yes" : "no");
                std::string appendfsync;
                switch (gAof.mode())
                {
                case AofMode::No:
                    appendfsync = "no";
                    break;
                case AofMode::EverySec:
                    appendfsync = "everysec";
                    break;
                case AofMode::Always:
                    appendfsync = "always";
                    break;
                }
                kvs.emplace_back("appendfsync", appendfsync);
                kvs.emplace_back("dir", "./data");
                kvs.emplace_back("dbfilename", "dump.rdb");
                kvs.emplace_back("save", "");
                kvs.emplace_back("timeout", "0");
                kvs.emplace_back("databases", "16");
                kvs.emplace_back("maxmemory", "0");
                std::string body;
                size_t elems = 0;
                if (pattern == "*")
                {
                    for (auto &p : kvs)
                    {
                        body += respBulkString(p.first);
                        body += respBulkString(p.second);
                        elems += 2;
                    }
                }
                else
                {
                    for (auto &p : kvs)
                    {
                        if (match(p.first))
                        {
                            body += respBulkString(p.first);
                            body += respBulkString(p.second);
                            elems += 2;
                        }
                    }
                }
                return "*" + std::to_string(elems) + "\r\n" + body;
            }
            else if (sub == "RESETSTAT")
            {
                if (respV._array.size() != 2)
                    return respError("ERR wrong number of arguments for 'CONFIG RESETSTAT'");
                return respSimpleString("OK");
            }
            else
            {
                return respError("ERR unsupported CONFIG subcommand");
            }
        }
        if (cmd == "INFO")
        {
            // INFO [section] -> ignore section for now
            std::string info;
            info.reserve(512);
            info += "# Server\r\nredis_version:0.1.0\r\nrole:master\r\n";
            info += "# Clients\r\nconnected_clients:0\r\n";
            info += "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\n";
            info += "# Persistence\r\naof_enabled:";
            info += (gAof.isEnabled() ? "1" : "0");
            info += "\r\naof_rewrite_in_progress:0\r\nrdb_bgsave_in_progress:0\r\n";
            info += "# Replication\r\nconnected_slaves:0\r\nmaster_repl_offset:" + std::to_string(gRepliBacklogOffset) + "\r\n";
            return respBulkString(info);
        }
        return respError("ERR unknown command");
    }
    //将字符串入队到conn的outChunks数组中
    static inline void enqueueOut(NetConnection &conn, std::string s)
    {
        if (!s.empty())
        {
            conn._outChunks.emplace_back(std::move(s));
        }
    }
    static const size_t kReplBacklogCap = 1024 * 4 * 1024;
    int64_t gReplBacklogStartOffset = 0;
    
    static std::string gReplBacklog{};
    
    //添加原始命令字符串到gReplBacklog中



    //将新增的字符串s添加到gReplBacklog的后面，如果添加后的总长度大于kReplBacklogCap规定的最大长度，那么erase掉前面多余长度的字符串
    static void appendToBacklog(const std::string &s)
    {
        // 对gReplBacklog限制最大容量，如果gReplBacklog本身长度加上s的长度小于等于容量上限，可以添加进去
        if (gReplBacklog.size() + s.size() <= kReplBacklogCap)
            gReplBacklog.append(s);
        // 否则切割去除掉前面多余的字符串，将新字符串拼接在gReplBacklog后面维持整个gReplBacklog最大长度不变，相当于gReplBacklog是一个队列，左边出右边进
        else
        {
            size_t need = s.size();
            if (need >= kReplBacklogCap)
            {
                gReplBacklog.assign(s.data() + (need - kReplBacklogCap), kReplBacklogCap);
            }
            else
            {
                size_t drop = (gReplBacklog.size() + need) - kReplBacklogCap;
                gReplBacklog.erase(0, drop);
                gReplBacklog.append(s);
            }
        }
        //当gReplBacklog的长度加上s的长度比kReplBacklogCap小时，gReplBacklogStartOffset会得到一个负数，这是允许的，因为我们最终需要的是一个相对位置
        //由于gRepliBacklogOffset是全局位置，而gReplBacklog.size()是有一个上限值的，所以当gRepliBacklogOffset比gReplBacklog.size()大时，gReplBacklogStartOffset就会是当前gReplBacklog的开始位置的全局值
        gReplBacklogStartOffset = gRepliBacklogOffset - static_cast<int64_t>(gReplBacklog.size());
    }
    extern std::sig_atomic_t gShouldStop;
    int Server::loop()
    {
        std::unordered_map<int, NetConnection> connsMap;
        std::vector<epoll_event> events(256);
        while (1)
        {
            if(gShouldStop)return 0;
            int nready = epoll_wait(_epollFd, events.data(), static_cast<int>(events.size()), -1);
            if (nready < 0)
            {
                // 被信号打断直接忽视再次恢复正常
                if (errno == EINTR)
                    continue;
                // 其他错误终止程序
                std::perror("epoll_wait error\n");
                return -1;
            }
            //std::cout<<"nready:"<<nready<<'\n';
            for (int i = 0; i < nready; i++)
            {
                int fd = events[i].data.fd;
                uint32_t ev = events[i].events;
                if (fd == _listenFd)
                {
                    // 监听fd只会触发EPOLLIN事件接收客户端连接建立请求
                    // 一次listenfd读事件的触发只是告诉程序至少有一个连接等待被处理，所以我们要循环accept直到全连接队列为空发生错误跳出循环
                    // 之前的事件驱动reactor网络模型我理解上有问题，几乎所有的服务器都是使用非阻塞listenfd
                    while (1)
                    {
                        sockaddr_in cliAddr{};
                        socklen_t addrLen = sizeof(sockaddr_in);
                        int cfd = accept(_listenFd, reinterpret_cast<sockaddr *>(&cliAddr), &addrLen);
                        if (cfd == -1)
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            std::perror("accept failed\n");
                            break;
                        }
                        //std::cout<<"fd:"<<cfd<<" "<<inet_ntoa(cliAddr.sin_addr)<<'\n';
                        // 设置cfd为非阻塞模式，方便后续的一次性循环读取数据，当内核写缓冲区满时也可以不阻塞返回
                        setNonBlock(cfd);
                        // 同时在高性能场景下，将cfd的tcp层面的Nagle 算法关闭，Nagle 算法是尽量减少小包的频繁发送，尽量凑足一个大包再一起发送，用网络带宽来换取数据传输及时性
                        // 就是说在Nagle算法开启的情况下，当本端发送一个小包时，如果对端没有回ack并且当前仍小于MSS，本端就不再发送新的数据包而是等待对端回应一个ack或者等待本端凑够一个MSS大小再一起发送，这样也是重传了之前未得回应的小包
                        // Nagle算法就是在tcp协议上新增的一个小包层的过滤
                        int on = 1;
                        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
                        addEpoll(_epollFd, cfd, EPOLLIN);
                        // 根基cfd索引可以直接找到对应的NetConnection
                        connsMap.emplace(cfd, NetConnection{cfd, std::string{}, std::vector<std::string>{}, 0, 0, RespParser{}, false});
                        //std::cout<<"listen connsMap cnt:"<<connsMap.size()<<'\n';
                    }
                    continue;
                }
                if (fd == _timerFd)
                {
                    while (1)
                    {
                        uint64_t count;
                        // 内核中有一个8字节的字段存储timerfd过期次数，read一次就重置这个字段为0
                        // 在timerfd为阻塞模式下，如果内核过期次数字段为0，那么read会阻塞，如果在非阻塞模式下，read会返回错误
                        ssize_t r = read(_timerFd, &count, sizeof(uint64_t));
                        if (r == -1)
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            std::perror("failed to read timerfd\n");
                            break;
                        }
                        if (r == 0)
                            break;
                        while(count--){
                            gStore.expireScanStep(64);
                        }
                    }
                    continue;
                }
                // 以下只有fd为clientfd才会执行
                // 先找到connsMap中fd对应的这个pair,然后拿到NetConnection
                auto it = connsMap.find(fd);
                if (it == connsMap.end())
                    continue;
                NetConnection &conn = it->second;
                // clientfd出现通道不可用或者严重错误，这里的EPOLLHUP不需要用户手动注册，如果fd被挂起，那么内核会自动将EPOLLHUP塞进epoll_wait返回的事件集中
                if ((ev & EPOLLHUP) || (ev & EPOLLERR))
                {
                    epoll_ctl(_epollFd, EPOLL_CTL_DEL, fd, nullptr);
                    //std::cout<<"epoll DEL fd:"<<fd<<'\n';
                    close(fd);
                    connsMap.erase(it);
                    continue;
                }
                // clientfd出现EPOLLIN事件
                if (ev & EPOLLIN)
                {
                    //std::cout<<"EPOLLIN connsMap cnt:"<<connsMap.size()<<'\n';
                    char buf[4096];
                    // 循环读
                    while (1)
                    {
                        ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
                        if (r > 0)
                        {
                            // 正常读取到数据就追加到conn的parser对象的string中准备解析
                            conn._parser.append(std::string_view{buf, static_cast<size_t>(r)});
                        }
                        else if (r == 0)
                        {
                            // 对端调用close发送fin包
                            // 那么结束这一次的循环读
                            ev |= EPOLLRDHUP;
                            break;
                        }
                        else
                        {
                            // 出错
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            std::perror("recv error\n");
                            ev |= EPOLLRDHUP;
                            break;
                        }
                    }

                    while (1)
                    {
                        // 解析客户端发来的命令并且返回原始字符串命令
                        auto maybe = conn._parser.tryParseOneWithRaw();
                        if (!maybe.has_value())
                            break;
                        const RespValue &v = maybe->first;
                        const std::string &raw = maybe->second;
                        if (v._type == RespType::Error)
                        {
                            enqueueOut(conn, respError(v._bulk));
                        }
                        else
                        {
                            if (v._type == RespType::Array && !v._array.empty() && (v._array[0]._type == RespType::SimpleString || v._array[0]._type == RespType::BulkString))
                            {
                                std::string cmd{};
                                cmd.reserve(v._array[0]._bulk.size());
                                // 将v._array[0]中的字符串转为大写再放入cmd中
                                // 其实redis是不区分这个命令大小写的，只不过为了代码的一致性，我们不管传来的命令是大写还是小写或者大小写结合，我们先将其全部转为大写再进行比较
                                for (auto ch : v._array[0]._bulk)
                                {
                                    cmd.push_back(static_cast<char>(::toupper(ch)));
                                }
                                // 在这里PSYNC是实现成判断增量同步的依据，实际上在新版的redis中，PSYNC是唯一的同步命令，不管从节点需要全量还是增量同步，都是发送PSYNC命令，然后从节点通过主节点的回复来判断具体是增量还是全量同步
                                if (cmd == "PSYNC")
                                {
                                    //std::cout<<"repli psync\n";
                                    if (v._array.size() == 2 && v._array[1]._type == RespType::BulkString)
                                    {
                                        int64_t offset = 0;
                                        auto [p, e] = std::from_chars(v._array[1]._bulk.data(), v._array[1]._bulk.data() + v._array[1]._bulk.size(), offset);
                                        if (e != std::errc{} || p != v._array[1]._bulk.data() + v._array[1]._bulk.size())
                                        {
                                            offset == -1;
                                        }
                                        if (offset >= gReplBacklogStartOffset && offset <= gRepliBacklogOffset)
                                        {
                                            // 从节点的offset落在[gReplBacklogStartOffset,gRepliBacklogOffset],那么可以增量同步
                                            //[gReplBacklogStartOffset,gRepliBacklogOffset]就像是一个容错窗口，如果从节点的offset还在这个窗口中，那么可以增量同步
                                            size_t start = static_cast<size_t>(offset - gReplBacklogStartOffset); // start就是offset相对于gReplBacklogStartOffset的偏移
                                            if (start < gReplBacklog.size())
                                            {
                                                // 如果这个偏移量没有越界
                                                conn.isReplica = true; // 标志conn为从节点
                                                // 回复一个"+offset number",这个回复是自定义的，只要我在myredis这个程序中约定好这个回复的收发就能解析
                                                std::string reply = std::string{"+OFFSET "} + std::to_string(gRepliBacklogOffset) + std::string{"\r\n"};
                                                enqueueOut(conn, reply);
                                                // 将偏移量后面的内容塞进发送队列中
                                                enqueueOut(conn, gReplBacklog.substr(start));
                                                continue;
                                            }
                                        }
                                    }
                                }
                                // 在这里是判断从节点需要全量同步
                                if (cmd == "SYNC")
                                {
                                    std::string err{};
                                    RdbOptions rdbOptionTmp = _config._rdb;
                                    if (!rdbOptionTmp._enabled)
                                        rdbOptionTmp._enabled = true;
                                    Rdb rdb{rdbOptionTmp};
                                    if (!rdb.save(gStore, err))
                                        enqueueOut(conn, respError("error with rdb save\n"));
                                    else
                                    {
                                        std::string path = rdb.path();
                                        int fd = ::open(path.c_str(), O_RDONLY);
                                        if (fd == -1)
                                            enqueueOut(conn, respError("can not open file"));
                                        
                                        else
                                        {   
                                            char buf[8192] = {'\0'};
                                            std::string content{};
                                            size_t rlen = 0;
                                            while ((rlen = ::read(fd, buf, sizeof(buf)))>0){
                                                content.append(buf, rlen);
                                            }
                                            close(fd);
                                            enqueueOut(conn, respBulkString(content));
                                            conn.isReplica = true;
                                            std::string off = "+OFFSET" + std::to_string(gRepliBacklogOffset) + "\r\n";
                                            enqueueOut(conn, std::move(off));
                                        }
                                    }
                                    //如果是repli_client，接下来就是执行continue,此举会导致不会执行tryFlushNow，也就是说不会直接发送回复，而是走EPOLLOUT路线
                                    //那么就会发生在EPOLLOUT那里将outChunks中数据完全发送之后发现hasPending(cfd)为false,然后就close(cfd),断开了这条连接
                                    continue;
                                }
                            }
                            // 处理命令
                            enqueueOut(conn, handleCommand(v, &raw,_config));
                            // 处理完之后立马将conn积攒的消息发送出去
                            tryFlushNow(fd, conn, ev);
                        }
                    }
                    //这是在读操作执行后的操作，也就是说读事件触发时说明可能有对redis的写操作，那么必须将这个写操作同步到所有从节点上
                    // 在执行完写操作后会将这个复制队列的内容放入复制积压缓冲区中
                    if (!gReplQueue.empty())
                    {
                        //std::cout<<"EPOLLIN and gReplQueue unempty\n";
                        //<<"conns cnt:"<<connsMap.size()<<'\n';
                        for (auto &[k, c] : connsMap)
                        {
                            //std::cout<<"coonsMap loop fd:"<<c._fd<<'\n';
                            if (!c.isReplica)
                                continue;
                            //std::cout<<"block1\n";
                            //std::cout<<"one is replica\n";
                            for (const auto &v : gReplQueue)
                            {
                                //先将命令转为原始命令格式
                                std::string cmd = toRespArray(v);
                                int64_t next_off = gRepliBacklogOffset + static_cast<int64_t>(cmd.size());
                                //组织一条offset命令准备回发
                                std::string off = "+OFFSET " + std::to_string(next_off) + "\r\n";
                                appendToBacklog(off);
                                appendToBacklog(cmd);
                                gRepliBacklogOffset = next_off;
                                //std::cout<<"data enter outChunks\n";
                                enqueueOut(c, off);
                                enqueueOut(c, cmd);
                            }
                            //std::cout<<"block2\n";
                            if (hasPending(c))
                            {
                                //std::cout<<"!gReplQueue.empty() and mod EPOLLOUT\n";
                                modEpoll(_epollFd, c._fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP);
                            }
                            //std::cout<<"block3\n";
                        }
                        gReplQueue.clear();
                    }
                    if (hasPending(conn))
                    {
                        //std::cout<<"repli conn mod EPOLLOUT\n";
                        modEpoll(_epollFd, conn._fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP);
                    }
                    if ((ev & EPOLLRDHUP) && !hasPending(conn))
                    {
                        epoll_ctl(_epollFd, EPOLL_CTL_DEL, fd, nullptr);
                        //std::cout<<"epoll del fd:"<<fd<<'\n';
                        close(fd);
                        connsMap.erase(it);
                        continue;
                    }
                }
                if (ev & EPOLLOUT)
                {
                    //std::cout<<"repli conn enter EPOLLOUT\n";
                    while (hasPending(conn))
                    {
                        const size_t maxIov = 64;
                        struct iovec iov[maxIov];
                        int iovcnt = 0;
                        size_t idx = conn._outIndex;
                        size_t off = conn._outOffset;
                        // 将_outChunks中的数据块全部送入iov中
                        while (idx < conn._outChunks.size() && iovcnt < (int)maxIov)
                        {
                            const std::string &s = conn._outChunks[idx];
                            const char *base = s.data();
                            size_t len = s.size();
                            if (off >= len)
                            {
                                ++idx;
                                off = 0;
                                continue;
                            }
                            iov[iovcnt].iov_base = (void *)(base + off);
                            iov[iovcnt].iov_len = len - off;
                            ++iovcnt;
                            ++idx;
                            off = 0;
                        }
                        if (iovcnt == 0)
                            break;
                        ssize_t w = ::writev(fd, iov, iovcnt);
                        //std::cout<<"conn write size:"<<w<<'\n';
                        if (w > 0)
                        {
                            size_t len = (size_t)w;
                            while (len > 0 && conn._outIndex < conn._outChunks.size())
                            {
                                std::string &str = conn._outChunks[conn._outIndex];
                                size_t avail = str.size() - conn._outOffset;
                                if (len < avail)
                                {
                                    conn._outOffset += len;
                                    len = 0;
                                }
                                else if (len >= avail)
                                {
                                    len -= avail;
                                    conn._outOffset = 0;
                                    conn._outIndex++;
                                }
                            }
                            if (conn._outIndex >= conn._outChunks.size())
                            {
                                // 说明_outChunks全部塞进iov中了，需要清楚资源
                                //std::cout<<"clear outChunks\n";
                                conn._outChunks.clear();
                                conn._outIndex = 0;
                                conn._outOffset = 0;
                            }
                        }
                        else if (w < 0 && (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK))
                        {
                            break;
                        }
                        else
                        {
                            std::perror("writev");
                            ev |= EPOLLRDHUP;
                            break;
                        }
                    }
                    //在检测到EPOLLOUT事件后，如果发现conn的outChunks发送完毕了，也就是没有数据可以写入内核发送缓冲区，那么说明该连接暂时处于非活跃的状态，可以关闭连接了
                    if (!hasPending(conn))
                    {
                        //如果conn没有数据可以发送，那么说明可能出问题了,可以考虑关闭连接
                        modEpoll(_epollFd,fd,EPOLLIN|EPOLLRDHUP|EPOLLHUP);
                        if(ev&EPOLLRDHUP){
                            epoll_ctl(_epollFd, EPOLL_CTL_DEL, fd, nullptr);
                            //std::cout<<"epoll delllll fd:"<<fd<<'\n';
                            close(fd);
                            connsMap.erase(it);
                            continue;
                        }
                        
                    }
                }
            }
        }
    }

    int Server::run()
    {
        //设置监听
        if (setupListen() == -1)
            return -1;
        //设置epoll I/O多路复用
        if (setupEpoll() == -1)
            return -1;
        //如果配置文件中rdb的enabled是真，那么开启rdb持久化
        if(_config._rdb._enabled){
            //给rdb设置rdb选项配置
            gRdb.setOptions(_config._rdb);
            //std::cout<<"rdb enable\n";
            std::string err;
            //载入rdb持久化文件到gStore中的数据结构中
            if(!gRdb.load(gStore,err)){
                std::perror("can not load rdb\n");
                return -1;
            }
            //std::cout<<"rdb load success\n";
        }
        //如果配置文件中的aof的enabled是真，那么开启aof持久化
        if(_config._aof._enabled){
            //std::cout<<"aof enabled\n";
            std::string err;
            //给aof设置aof选项配置并且初始化aof类中的成员变量，在初始化其中的线程成员变量时开启线程跑writeLoop方法
            if(!gAof.init(_config._aof,err)){
                std::perror("error with aof init");
                return -1;
            }
            //将aof持久化文件中的内容读取出来并且执行相应的操作使得数据恢复到gStore中的数据结构中
            if(!gAof.load(gStore,err)){
                std::perror("error with aof load");
                return -1;
            }
        }
        //std::cout<<"rdb aof all ready\n";
        //将_config给到ReplicaClient
        ReplicaClient replica{_config};
        //如果配置参数的enabled是真的话，开启一个新线程去做主从同步工作
        replica.start();
        //开启事件循环
        int rc=loop();
        replica.stop();
        return rc;
    }
}
