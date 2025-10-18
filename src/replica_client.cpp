#include "../include/replica_client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include "../include/aof.h"
#include "../include/resp.h"
#include <iostream>
#include "../include/rdb.h"
#include <unistd.h>
namespace myredis
{
    extern KeyValueStore gStore;
    ReplicaClient::ReplicaClient(const ServerConfig &conf) : _conf{conf}
    {
    }
    ReplicaClient::~ReplicaClient()
    {
        stop();
    }
    // 将_running设置为true,开启replica线程
    void ReplicaClient::start()
    {
        if (!_conf._replica._enabled)
            return;
        _running = true;
        _thread = std::thread([this]
                              { threadMain(); });
    }
    // 结束replica线程
    void ReplicaClient::stop()
    {
        if (_thread.joinable())
        {
            _running = false;
            _thread.join();
        }
    }
    void ReplicaClient::threadMain()
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return;
        // 将ReplicaOptions中对端的host和port绑定到addr中
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(_conf._replica._masterPort);
        ::inet_pton(AF_INET, _conf._replica._masterHost.c_str(), &addr.sin_addr);
        // 将fd连接到对端
        if (::connect(fd, (sockaddr *)&addr, sizeof(addr)) < 0)
        {
            ::close(fd);
            return;
        }

        // 发送SYNC/PSYNC
        std::string first;
        if (_lastOffset > 0)
        {
            first = toRespArray({std::string{"PSYNC"}, std::to_string(_lastOffset)});
        }
        else
        {
            first = toRespArray({std::string{"SYNC"}});
        }
        ::send(fd, first.data(), first.size(), 0);
        RespParser parser;
        std::string buf(8192, '\0');
        while (_running)
        {
            ssize_t r = ::recv(fd, buf.data(), buf.size(), 0);
            if (r <= 0)
                break;
            parser.append(std::string_view{buf.c_str(), static_cast<size_t>(r)});
            while (1)
            {
                std::optional<RespValue> v = parser.tryParseOne();
                if (!v.has_value())
                {
                    break;
                }
                if (v->_type == RespType::BulkString)
                {
                    RdbOptions ropts = _conf._rdb;
                    if (!ropts._enabled)
                        ropts._enabled = true;
                    Rdb r(ropts);
                    std::string path = r.path();
                    FILE *f = ::fopen(path.c_str(), "wb");
                    if (!f)
                        return;
                    fwrite(v->_bulk.data(), 1, v->_bulk.size(), f);
                    fclose(f);
                    std::string err;
                    r.load(gStore, err);
                }
                else if (v->_type == RespType::Array)
                {
                    if (v->_array.empty())
                        continue;
                    std::string cmd;
                    for (auto &c : v->_array[0]._bulk)
                        cmd.push_back(static_cast<char>(::toupper(c)));
                    if (cmd == "SET" && v->_array.size() == 3)
                    {
                        gStore.set(v->_array[1]._bulk, v->_array[2]._bulk);
                    }
                    else if (cmd == "DEL" && v->_array.size() >= 2)
                    {
                        std::vector<std::string> args;
                        for (int i = 1; i < v->_array.size(); i++)
                            args.emplace_back(v->_array[i]._bulk);
                        gStore.del(args);
                    }
                    else if (cmd == "EXPIRE" && v->_array.size() == 3)
                    {
                        int64_t s = std::stoll(v->_array[2]._bulk);
                        gStore.expire(v->_array[1]._bulk, s);
                    }
                    else if (cmd == "HSET" && v->_array.size() >= 4 && !(v->_array.size() % 2))
                    {
                        std::vector<std::string> args;
                        for (int i = 2; i < v->_array.size(); i++)
                            args.push_back(v->_array[i]._bulk);
                        gStore.hset(v->_array[1]._bulk, args);
                    }
                    else if (cmd == "HDEL" && v->_array.size() >= 3)
                    {
                        std::vector<std::string> fs;
                        for (size_t i = 2; i < v->_array.size(); ++i)
                            fs.emplace_back(v->_array[i]._bulk);
                        gStore.hdel(v->_array[1]._bulk, fs);
                    }
                    else if (cmd == "ZADD" && v->_array.size() >= 4 && !(v->_array.size() % 2))
                    {
                        std::vector<std::pair<double, std::string>> args;
                        for (int i = 2; i < v->_array.size(); i += 2)
                        {
                            double sc = std::stod(v->_array[i]._bulk);
                            std::string member = v->_array[i + 1]._bulk;
                            args.push_back({sc, member});
                        }
                        gStore.zadd(v->_array[1]._bulk, args);
                    }
                    else if (cmd == "ZREM" && v->_array.size() >= 3)
                    {
                        std::vector<std::string> ms;
                        for (size_t i = 2; i < v->_array.size(); ++i)
                            ms.emplace_back(v->_array[i]._bulk);
                        gStore.zrem(v->_array[1]._bulk, ms);
                    }
                    else if (v->_type == RespType::SimpleString)
                    {
                        // parse +OFFSET <num>
                        const std::string &s = v->_bulk;
                        if (s.rfind("OFFSET ", 0) == 0)
                        {
                            try
                            {
                                _lastOffset = std::stoll(s.substr(8));
                            }
                            catch (...)
                            {
                            }
                        }
                    }
                }
            }
        }
        ::close(fd);
    }
}
