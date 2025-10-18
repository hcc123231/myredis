#include "../include/config_loader.h"
#include <fstream>
namespace myredis
{
    static std::string trim(const std::string &s)
    {
        size_t i = 0, j = s.size();
        while (i < j && std::isspace(static_cast<unsigned char>(s[i])))
            ++i;
        while (j > i && std::isspace(static_cast<unsigned char>(s[j - 1])))
            --j;
        return s.substr(i, j - i);
    }
    //根据path将文件内容解析出来填写传入的ServerConfig引用变量
    bool loadConfigFromFile(const std::string &path, ServerConfig &cfg, std::string &err)
    {
        std::ifstream in(path);
        if (!in.good())
        {
            err = "open config failed: " + path;
            return false;
        }
        std::string line;
        int lineno = 0;
        while (std::getline(in, line))
        {
            ++lineno;
            std::string t = trim(line);
            if (t.empty() || t[0] == '#')
                continue;
            auto pos = t.find('=');
            if (pos == std::string::npos)
            {
                err = "invalid line " + std::to_string(lineno);
                return false;
            }
            std::string key = trim(t.substr(0, pos));
            std::string val = trim(t.substr(pos + 1));
            if (key == "port")
            {
                try
                {
                    cfg._port = static_cast<uint16_t>(std::stoi(val));
                }
                catch (...)
                {
                    err = "invalid port at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "bind_address")
            {
                cfg._bindAddr = val;
            }
            else if (key == "aof.enabled")
            {
                cfg._aof._enabled = (val == "1" || val == "true" || val == "yes");
            }
            else if (key == "aof.mode")
            {
                if (val == "no")
                    cfg._aof._mode = AofMode::No;
                else if (val == "everysec")
                    cfg._aof._mode = AofMode::EverySec;
                else if (val == "always")
                    cfg._aof._mode = AofMode::Always;
                else
                {
                    err = "invalid aof.mode at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "aof.dir")
            {
                cfg._aof._dir = val;
            }
            else if (key == "aof.filename")
            {
                cfg._aof._filename = val;
            }
            else if (key == "aof.batch_bytes")
            {
                try
                {
                    cfg._aof._batch_bytes = static_cast<size_t>(std::stoull(val));
                }
                catch (...)
                {
                    err = "invalid aof.batch_bytes at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "aof.batch_wait_us")
            {
                try
                {
                    cfg._aof._batch_wait_us = std::stoi(val);
                }
                catch (...)
                {
                    err = "invalid aof.batch_wait_us at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "aof.prealloc_bytes")
            {
                try
                {
                    cfg._aof._prealloc_bytes = static_cast<size_t>(std::stoull(val));
                }
                catch (...)
                {
                    err = "invalid aof.prealloc_bytes at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "aof.sync_interval_ms")
            {
                try
                {
                    cfg._aof._sync_interval_ms = std::stoi(val);
                }
                catch (...)
                {
                    err = "invalid aof.sync_interval_ms at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "aof.use_sync_file_range")
            {
                cfg._aof._use_sync_file_range = (val == "1" || val == "true" || val == "yes");
            }
            else if (key == "aof.sfr_min_bytes")
            {
                try
                {
                    cfg._aof._sfr_min_bytes = static_cast<size_t>(std::stoull(val));
                }
                catch (...)
                {
                    err = "invalid aof.sfr_min_bytes at line " + std::to_string(lineno);
                    return false;
                }
            }
            else if (key == "aof.fadvise_dontneed_after_sync")
            {
                cfg._aof._fadvise_dontneed_after_sync = (val == "1" || val == "true" || val == "yes");
            }
            else if (key == "rdb.enabled")
            {
                cfg._rdb._enabled = (val == "1" || val == "true" || val == "yes");
            }
            else if (key == "rdb.dir")
            {
                cfg._rdb._dir = val;
            }
            else if (key == "rdb.filename")
            {
                cfg._rdb._filename = val;
            }
            else if (key == "replica.enabled")
            {
                cfg._replica._enabled = (val == "1" || val == "true" || val == "yes");
            }
            else if (key == "replica.master_host")
            {
                cfg._replica._masterHost = val;
            }
            else if (key == "replica.master_port")
            {
                try
                {
                    cfg._replica._masterPort = static_cast<uint16_t>(std::stoi(val));
                }
                catch (...)
                {
                    err = "invalid replica.master_port at line " + std::to_string(lineno);
                    return false;
                }
            }
            else
            {
                // ignore unknown keys for forward compatibility
            }
        }
        return true;
    }
}
