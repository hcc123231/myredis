#include "../include/rdb.h"
#include <system_error>
#include <fcntl.h>
#include <unistd.h>
#include<iostream>
#include <filesystem>
namespace myredis
{
    // 拼接文件路径
    static std::string joinPath(const std::string &dir, const std::string &filename)
    {
        if (dir.empty())
            return filename;
        if (dir.back() == '/')
            return dir + filename;
        return dir + '/' + filename;
    }
    // 获取文件路径
    std::string Rdb::path() const
    {
        return joinPath(_opts._dir, _opts._filename);
    }
    // rdb持久化存储,将内存数据写入磁盘
    bool Rdb::save(const KeyValueStore &store, std::string &err) const
    {
        //std::cout<<"save called\n";
        // 先判断rdb选项参数中的enabled是否为true，只有true才能执行save操作
        if (!_opts._enabled)
            return true;
        std::error_code ec;
        if (!std::filesystem::create_directories(_opts._dir, ec))
        {
            err = ec.message();
        }
        int fd = ::open(path().c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd < 0)
        {
            err = "can not open";
            return false;
        }
        auto snapshootStr = store.snapshot();
        auto snapshootHash = store.snapshotHash();
        auto snapshootZset = store.snapshotZset();

        // rdb持久化，使用自定义协议
        //  head: MRDB2
        //  Strings: STR count\n then per line: klen key vlen value expire_ms\n
        //  Hash: HASH count\n then per hash: klen key expire_ms num_fields\n then num_fields lines: flen field vlen value\n
        //  ZSet: ZSET count\n then per zset: klen key expire_ms num_items\n then num_items lines: score member_len member\n
        //std::cout<<"path:"<<path()<<'\n';
        std::string head{"MRDB2\n"};
        if (::write(fd, head.c_str(), head.size()) < 0)
        {
            ::close(fd);
            err = "failed to write head\n";
            return false;
        }
        // str
        std::string headLine = std::string{"STR "} + std::to_string(snapshootStr.size()) + std::string{"\n"};
        if (::write(fd, headLine.c_str(), headLine.size()) < 0)
        {
            ::close(fd);
            err = "failed to write str head\n";
            return false;
        }
        for (const auto &[k, v] : snapshootStr)
        {
            std::string strBodyLine{};
            strBodyLine.append(std::to_string(k.size())).append(" ").append(k).append(" ").append(std::to_string(v._value.size())).append(" ").append(v._value).append(" ").append(std::to_string(v._expireAtMs)).append("\n");
            if (::write(fd, strBodyLine.c_str(), strBodyLine.size()) < 0)
            {
                ::close(fd);
                err = "failed to write str bodyLines\n";
                return false;
            }
        }
        // hash
        headLine = std::string{"HASH "} + std::to_string(snapshootHash.size()) + std::string{"\n"};
        if (::write(fd, headLine.c_str(), headLine.size()) < 0)
        {
            ::close(fd);
            err = "failed to write hash head\n";
            return false;
        }
        for (const auto &[k, v] : snapshootHash)
        {
            std::string hashHeadLine{};
            hashHeadLine.append(std::to_string(k.size())).append(" ").append(k).append(" ").append(std::to_string(v._expireAtMs)).append(" ").append(std::to_string(v._hashTable.size())).append("\n");
            if (::write(fd, hashHeadLine.c_str(), hashHeadLine.size()) < 0)
            {
                ::close(fd);
                err = "failed to write hash head line\n";
                return false;
            }
            for (const auto &[hk, hv] : v._hashTable)
            {
                std::string hashBodyLine{};
                hashBodyLine.append(std::to_string(hk.size())).append(" ").append(hk).append(" ").append(std::to_string(hv.size())).append(" ").append(hv).append("\n");
                if (::write(fd, hashBodyLine.c_str(), hashBodyLine.size()) < 0)
                {
                    ::close(fd);
                    err = "failed to write hash body lines\n";
                    return false;
                }
            }
        }
        // zset
        headLine = std::string{"ZSET "} + std::to_string(snapshootZset.size()) + std::string{"\n"};
        if (::write(fd, headLine.c_str(), headLine.size()) < 0)
        {
            ::close(fd);
            err = "failed to write zset head\n";
            return false;
        }
        for (const auto &data : snapshootZset)
        {
            std::string zsetHeadLine{};
            zsetHeadLine.append(std::to_string(data._key.size())).append(" ").append(data._key).append(" ").append(std::to_string(data._expireAtMs)).append(" ").append(std::to_string(data._value.size())).append(" ").append("\n");
            if (::write(fd, zsetHeadLine.c_str(), zsetHeadLine.size()) < 0)
            {
                ::close(fd);
                err = "failed to write zset head line\n";
                return false;
            }
            for (const auto &[s, m] : data._value)
            {
                std::string zsetBodyLines{};
                zsetBodyLines.append(std::to_string(s)).append(" ").append(std::to_string(m.size())).append(" ").append(m).append("\n");
                if (::write(fd, zsetBodyLines.c_str(), zsetBodyLines.size()) < 0)
                {
                    ::close(fd);
                    err = "failed to write zset body line\n";
                    return false;
                }
            }
        }
        ::fsync(fd);
        ::close(fd);
        return true;
    }
    // 将磁盘上的rdb文件内容导入程序，成为内存数据
    bool Rdb::load(KeyValueStore &store, std::string &err) const
    {
        if (!_opts._enabled)
            return true;
        int fd = ::open(path().c_str(), O_RDONLY);
        if (fd < 0)
            return true;
        std::string data;
        data.resize(1 << 20); // 1024*1024->1MB
        std::string file;
        // 读出磁盘文件内容到string file中
        while (true)
        {
            ssize_t r = ::read(fd, data.data(), data.size());
            if (r < 0)
            {
                ::close(fd);
                err = "read rdbfile";
                return false;
            }
            if (r == 0)
                break;
            file.append(data.data(), static_cast<size_t>(r));
        }
        ::close(fd);
        size_t pos = 0; // 每次查找string的开始位置
        // 读取file中的一行，这个一行就是指'\n'结束之前内容，然后将这个截取出来的字符串分配给传入参数out
        auto readLine = [&](std::string &out) -> bool
        {
            size_t e = file.find('\n', pos);
            if (e == std::string::npos)
                return false;
            out.assign(file.data() + pos, e - pos);
            pos = e + 1;
            return true;
        };
        std::string line;
        // 先读取第一行，看看是否为之前写入的"MRDB"这个标志
        if (!readLine(line))
        {
            err = "readline error";
            return false;
        }
        // MRDB1只针对字符串类型的数据结构
        if (line == "MRDB1")
        {
            if (!readLine(line))
            {
                err = "error with readLine";
                return false;
            }
            int count = std::stoi(line);
            for (int i = 0; i < count; ++i)
            {
                if (!readLine(line))
                {
                    err = "error with readLine";
                    return false;
                }
                size_t p = 0;
                auto nextToken = [&](std::string &token) -> bool
                {
                    size_t s = p;
                    while (s < line.size() && line[s] == ' ')
                        ++s;
                    size_t q = line.find(' ', s);
                    if (q == std::string::npos)
                    {
                        token = line.substr(s);
                        p = line.size();
                        return true;
                    }
                    token = line.substr(s, q - s);
                    p = q + 1;
                    return true;
                };
                std::string keyLenS;
                nextToken(keyLenS);
                int klen = std::stoi(keyLenS);
                std::string key = line.substr(p, static_cast<size_t>(klen));
                p += static_cast<size_t>(klen) + 1;
                std::string valLenS;
                nextToken(valLenS);
                int vlen = std::stoi(valLenS);
                std::string val = line.substr(p, static_cast<size_t>(vlen));
                p += static_cast<size_t>(vlen) + 1;
                std::string expireS;
                nextToken(expireS);
                int64_t expire = std::stoll(expireS);
                store.setWithExpireAtMs(key, val, expire);
            }
            return true;
        }
        // MRDB2就针对str,hash,zset都有覆盖
        if (line != "MRDB2")
        {
            err = "err protocol";
            return false;
        }
        // str section
        if (!readLine(line))
        {
            err = "no str section";
            return false;
        }
        if (line.rfind("STR", 0) != 0)
        {
            err = "no str tag";
            return false;
        }
        int strCount = std::stoi(line.substr(4));
        for (int i = 0; i < strCount; i++)
        {
            if (!readLine(line))
            {
                err = "error with readLine";
                return false;
            }
            size_t p = 0;
            auto nextToken = [&](std::string &token) -> bool
            {
                size_t s = p;
                while (s < line.size() && line[s] == ' ')
                    ++s;
                size_t q = line.find(' ', s);
                if (q == std::string::npos)
                {
                    token = line.substr(s);
                    p = line.size();
                    return true;
                }
                token = line.substr(s, q - s);
                p = q + 1;
                return true;
            };
            std::string keyLenS;
            nextToken(keyLenS);
            int klen = std::stoi(keyLenS);
            std::string key = line.substr(p, static_cast<size_t>(klen));
            p += static_cast<size_t>(klen) + 1;
            std::string valLenS;
            nextToken(valLenS);
            int vlen = std::stoi(valLenS);
            std::string val = line.substr(p, static_cast<size_t>(vlen));
            p += static_cast<size_t>(vlen) + 1;
            std::string expireS;
            nextToken(expireS);
            int64_t expire = std::stoll(expireS);
            store.setWithExpireAtMs(key, val, expire);
        }
        // hash section
        if (!readLine(line))
        {
            err = "no hash section";
            return false;
        }
        if (line.rfind("HASH", 0) != 0)
        {
            err = "no hash tag";
            return false;
        }
        int hashCount = std::stoi(line.substr(5));
        for (int i = 0; i < hashCount; i++)
        {
            if (!readLine(line))
            {
                err = "error with readLine";
                return false;
            }
            size_t p = 0;
            auto nextToken = [&](std::string &token) -> bool
            {
                size_t s = p;
                while (s < line.size() && line[s] == ' ')
                    ++s;
                size_t q = line.find(' ', s);
                if (q == std::string::npos)
                {
                    token = line.substr(s);
                    p = line.size();
                    return true;
                }
                token = line.substr(s, q - s);
                p = q + 1;
                return true;
            };
            std::string keyLenS;
            nextToken(keyLenS);
            int klen = std::stoi(keyLenS);
            std::string key = line.substr(p, static_cast<size_t>(klen));
            p += static_cast<size_t>(klen) + 1;
            std::string expire;
            nextToken(expire);
            int64_t exp = std::stoll(expire);
            std::string nfieldsS;
            nextToken(nfieldsS);
            int nfields = std::stoi(nfieldsS);
            bool hasAny = false;
            std::vector<std::pair<std::string, std::string>> fvs;
            fvs.reserve(nfields);
            for (int j = 0; j < nfields; ++j)
            {
                if (!readLine(line))
                {
                    err = "hash read fields failed";
                    return false;
                }
                size_t q = 0;
                auto nextTok2 = [&](std::string &tok) -> bool
                { size_t s = q; while (s < line.size() && line[s]==' ') ++s; size_t x = line.find(' ', s); if (x == std::string::npos) { tok = line.substr(s); q = line.size(); return true; } tok = line.substr(s, x-s); q = x+1; return true; };
                std::string flen_s;
                nextTok2(flen_s);
                int flen = std::stoi(flen_s);
                std::string field = line.substr(q, static_cast<size_t>(flen));
                q += static_cast<size_t>(flen) + 1;
                std::string vlen_s;
                nextTok2(vlen_s);
                int vlen = std::stoi(vlen_s);
                std::string val = line.substr(q, static_cast<size_t>(vlen));
                fvs.emplace_back(std::move(field), std::move(val));
                hasAny = true;
            }
            if (hasAny)
            {
                std::vector<std::string> tmp;
                tmp.reserve(2 * fvs.size());
                for (const auto &[k, v] : fvs)
                {
                    tmp.emplace_back(k);
                    tmp.emplace_back(v);
                }
                store.hset(key, tmp);
                if (exp >= 0)
                {
                    store.setHashExpireAtMs(key, exp);
                }
            }
        }
        // zset section
        if (!readLine(line))
        {
            err = "no zset section";
            return false;
        }
        if (line.rfind("ZSET", 0) != 0)
        {
            err = "no zset tag";
            return false;
        }
        int zsetCount = std::stoi(line.substr(5));
        for (int i = 0; i < zsetCount; ++i)
        {
            if (!readLine(line))
            {
                err = "error with readLine";
                return false;
            }
            size_t p = 0;
            auto nextToken = [&](std::string &token) -> bool
            {
                size_t s = p;
                while (s < line.size() && line[s] == ' ')
                    ++s;
                size_t q = line.find(' ', s);
                if (q == std::string::npos)
                {
                    token = line.substr(s);
                    p = line.size();
                    return true;
                }
                token = line.substr(s, q - s);
                p = q + 1;
                return true;
            };
            std::string keyLenS;
            nextToken(keyLenS);
            int klen = std::stoi(keyLenS);
            std::string key = line.substr(p, static_cast<size_t>(klen));
            p += static_cast<size_t>(klen) + 1;
            std::string expire;
            nextToken(expire);
            int64_t exp = std::stoll(expire);
            std::string nitemsS;
            nextToken(nitemsS);
            int nitems = std::stoi(nitemsS);
            bool hasAny = false;
            std::vector<std::pair<double,std::string>> itemsVec;
            itemsVec.reserve(nitems);
            for (int j = 0; j < nitems; ++j)
            {
                if (!readLine(line))
                {
                    err = "zset read fields failed";
                    return false;
                }
                size_t q = 0;
                auto nextTok2 = [&](std::string &tok) -> bool
                { size_t s = q; while (s < line.size() && line[s]==' ') ++s; size_t x = line.find(' ', s); if (x == std::string::npos) { tok = line.substr(s); q = line.size(); return true; } tok = line.substr(s, x-s); q = x+1; return true; };
                std::string score_s;
                nextTok2(score_s);
                double sc = std::stod(score_s);
                std::string mlen_s;
                nextTok2(mlen_s);
                int ml = std::stoi(mlen_s);
                std::string member = line.substr(q, static_cast<size_t>(ml));
                itemsVec.push_back({sc,member});
            }
            store.zadd(key,itemsVec);
            if(exp>=0)store.setZsetExpireAtMs(key,exp);
        }
        return true;
    }

}