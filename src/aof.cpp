#include "../include/aof.h"
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <sys/uio.h>
#include <iostream>
#include "../include/kv.h"
namespace myredis
{
    AofLogger::~AofLogger()
    {
        shutdown();
    }
    void AofLogger::shutdown()
    {
        _running.store(false);
        _stop.store(true);
        _cv.notify_all();
        if (_writeThread.joinable())
            _writeThread.join();
        if (_fd > 0)
        {
            ::fdatasync(_fd);
            ::close(_fd);
            _fd = -1;
        }
    }
    // 这个循环是由aof类单独开启一个线程执行的，_writeIsPaused，_queue，_pendBytes等等这些变量会被更改
    void AofLogger::writeLoop()
    {
        std::perror("enter writeLoop");
        const size_t kBatchBytes = _opt._batch_bytes > 0 ? _opt._batch_bytes : (64 * 1024);
        const size_t kMaxIov = 64;
        const auto kWaitMcs = std::chrono::microseconds(_opt._batch_wait_us > 0 ? _opt._batch_wait_us : 1000);

        // AofItem包含一个字符串和一个int64_t类型的序列号
        std::vector<AofItem> local;
        local.reserve(256);
        while (!_stop.load())
        {
            // std::cout<<"queue size:"<<_queue.size()<<'\n';
            // rewrite的过程中可能会设置pauseWrite为true
            if (_pauseWrite.load())
            {
                std::unique_lock<std::mutex> lock(_pauseMutex);
                _writeIsPaused = true;
                _pauseCv.notify_all();
                _pauseCv.wait(lock, [&]
                              { return !_pauseWrite.load() || _stop.load(); });
                _writeIsPaused = false;
                if (_stop.load())
                    break;
            }
            local.clear();
            size_t bytes = 0;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                if (_queue.empty())
                {
                    _cv.wait_for(lock, kWaitMcs, [&]
                                 { return _stop.load() || !_queue.empty(); });
                }
                while (!_queue.empty() && (bytes < kBatchBytes) && static_cast<int>(local.size()) < kMaxIov)
                {
                    local.emplace_back(std::move(_queue.front()));
                    bytes += local.back()._item.size();
                    _queue.pop_front();
                }
                if (_pendBytes >= bytes)
                    _pendBytes -= bytes;
                else
                    _pendBytes = 0;
            }
            if (local.empty())
            {
                if (_opt._mode == AofMode::EverySec)
                {
                    auto now = std::chrono::steady_clock::now();
                    auto interval = std::chrono::milliseconds(_opt._sync_interval_ms > 0 ? _opt._sync_interval_ms : 1000); // 时间间隔
                    if (now - _lastSyncTimepoint >= interval)
                    {
                        // 当前时间点减去上一个同步时间点要大于时间间隔的话，需要将数据同步到磁盘中
                        if (_fd >= 0)
                            ::fdatasync(_fd);
                        _lastSyncTimepoint = now;
                    }
                }
                continue;
            }

            // 组织iovec
            struct iovec iov[kMaxIov];
            int iovcnt = 0;
            // 将local中所有的数据字符串数据塞入iov中
            for (auto &it : local)
            {
                if (iovcnt > kMaxIov)
                    break;
                iov[iovcnt].iov_base = it._item.data();
                iov[iovcnt].iov_len = it._item.size();
                ++iovcnt;
            }
            int startIndex = 0;  // 表示当前正在写的iov开始元素的下标
            size_t startOff = 0; // 表示当前正在写的iov开始元素的字符串偏移量
            while (startIndex < iovcnt)
            {
                ssize_t w = ::writev(_fd, &iov[startIndex], iovcnt - startIndex);
                if (w < 0)
                {
                    // 可能是内核发送缓冲区无空闲空间，或者多线程竞争下，带来的写失败，所以需要休眠一段时间等待发送缓冲区空出来
                    ::usleep(1000);
                    break;
                }
                // 根据这次writev的返回值来决定iov的startIndex和startOff
                size_t rem = static_cast<size_t>(w);
                while (rem > 0 && startIndex < iovcnt)
                {
                    size_t avail = iov[startIndex].iov_len - startOff;
                    if (rem < avail)
                    {
                        startOff += rem;
                        iov[startIndex].iov_base = static_cast<char *>(iov[startIndex].iov_base) + rem;
                        iov[startIndex].iov_len = avail - rem;
                        rem = 0;
                    }
                    else
                    {
                        rem -= avail;
                        ++startIndex;
                        startOff = 0;
                    }
                }
                if (w == 0)
                    break;
            }
#ifdef __linux__
            if (_opt._use_sync_file_range && bytes >= _opt._sfr_min_bytes)
            {

                // 对最近写入的区间进行提示。这里为了简化，使用整个文件范围（可能较重），可进一步优化记录 offset
                off_t cur = ::lseek(_fd, 0, SEEK_END);
                if (cur > 0)
                {
                    // 提示内核把 [cur-bytes, cur) 写回磁盘
                    off_t start = cur - static_cast<off_t>(bytes);
                    if (start < 0)
                        start = 0;
                    // SYNC_FILE_RANGE_WRITE: 发起写回请求但不等待完成
                    (void)::sync_file_range(_fd, start, static_cast<off_t>(bytes), SYNC_FILE_RANGE_WRITE);
                }
            }
#endif
            if (_opt._mode == AofMode::Always)
            {
                ::fdatasync(_fd);
#ifdef __linux__
                if (_opt._fadvise_dontneed_after_sync)
                {
                    off_t cur2 = ::lseek(_fd, 0, SEEK_END);
                    if (cur2 > 0)
                    {
                        (void)::posix_fadvise(_fd, 0, cur2, POSIX_FADV_DONTNEED);
                    }
                }
#endif
                int64_t maxSeq = 0;
                for (auto &it : local)
                    maxSeq = std::max(maxSeq, it._seq);
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _lastSyncSqe = std::max(_lastSyncSqe, maxSeq);
                }
                _cvCommit.notify_all();
            }
            else if (_opt._mode == AofMode::EverySec)
            {
                auto now = std::chrono::steady_clock::now();
                auto interval = std::chrono::milliseconds(_opt._sync_interval_ms > 0 ? _opt._sync_interval_ms : 1000);
                if (now - _lastSyncTimepoint >= interval)
                {
                    ::fdatasync(_fd);
                    _lastSyncTimepoint = now;
#ifdef __linux__
                    if (_opt._fadvise_dontneed_after_sync)
                    {
                        off_t cur2 = ::lseek(_fd, 0, SEEK_END);
                        if (cur2 > 0)
                        {
                            (void)::posix_fadvise(_fd, 0, cur2, POSIX_FADV_DONTNEED);
                        }
                    }
#endif
                }
            }
        }
        // 退出前flush
        if (_fd >= 0)
        {
            // 把剩余队列写完
            while (true)
            {
                std::vector<AofItem> rest;
                size_t bytes = 0;
                {
                    std::lock_guard<std::mutex> lg(_mutex);
                    while (!_queue.empty() && (int)rest.size() < 64)
                    {
                        rest.emplace_back(std::move(_queue.front()));
                        bytes += rest.back()._item.size();
                        _queue.pop_front();
                    }
                    if (_pendBytes >= bytes)
                        _pendBytes -= bytes;
                    else
                        _pendBytes = 0;
                }
                if (rest.empty())
                    break;
                struct iovec iov2[64];
                int n = 0;
                for (auto &it : rest)
                {
                    iov2[n].iov_base = const_cast<char *>(it._item.data());
                    iov2[n].iov_len = it._item.size();
                    ++n;
                }
                int start_idx2 = 0;
                size_t start_off2 = 0;
                while (start_idx2 < n)
                {
                    ssize_t w2 = ::writev(_fd, &iov2[start_idx2], n - start_idx2);
                    if (w2 < 0)
                    {
                        if (errno == EINTR || errno == EAGAIN)
                            continue;
                        else
                            break;
                    }
                    size_t rem2 = static_cast<size_t>(w2);
                    while (rem2 > 0 && start_idx2 < n)
                    {
                        size_t avail2 = iov2[start_idx2].iov_len - start_off2;
                        if (rem2 < avail2)
                        {
                            start_off2 += rem2;
                            iov2[start_idx2].iov_base = static_cast<char *>(iov2[start_idx2].iov_base) + rem2;
                            iov2[start_idx2].iov_len = avail2 - rem2;
                            rem2 = 0;
                        }
                        else
                        {
                            rem2 -= avail2;
                            ++start_idx2;
                            start_off2 = 0;
                        }
                    }
                    if (w2 == 0)
                        break;
                }
            }
            ::fdatasync(_fd);
        }
    }
    bool AofLogger::init(const AofOptions &opt, std::string &err)
    {
        _opt = opt;
        if (!_opt._enabled)
        {
            std::perror("init aof unenabled");
            return true;
        }

        std::error_code ec;
        // 这是c++17提供的标准创建目录
        std::filesystem::create_directories(_opt._dir, ec);
        // error_code默认情况下不出错是0
        if (ec)
        {
            err = "mkdir failed:" + _opt._dir;
            return false;
        }
        _fd = ::open(path().c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
        if (_fd < 0)
        {
            err = "failed to create file:" + path();
            return false;
        }
#ifdef __linux__
        // 预分配文件空间防止因为磁盘空间不足导致写入失败
        if (_opt._prealloc_bytes > 0)
        {
            //std::cout << "prealloc size:" << _opt._prealloc_bytes << '\n';
            // posix_fallocate(_fd, 0, static_cast<off_t>(_opt._prealloc_bytes));
        }
#endif
        off_t offpos = lseek(_fd, 0, SEEK_CUR);
        //std::cout << "fd current pos:" << offpos << '\n';
        _running.store(true);
        _stop.store(false);
        _writeThread = std::thread(&AofLogger::writeLoop, this);
        return true;
    }
    bool AofLogger::load(KeyValueStore &store, std::string &err)
    {
        std::perror("aof load");
        if (!_opt._enabled)
        {
            std::perror("aof load enabled false");
            return true;
        }

        int rfd = ::open(path().c_str(), O_RDONLY);
        if (rfd < 0)
        {
            // not fatal if file does not exist
            std::perror("load can not open file");
            return true;
        }
        std::string buf;
        buf.resize(1 << 20);
        std::string data;
        while (true)
        {
            ssize_t r = ::read(rfd, buf.data(), buf.size());
            if (r < 0)
            {
                err = "read AOF failed";
                ::close(rfd);
                return false;
            }
            if (r == 0)
                break;
            // std::cout<<"size:"<<r<<" "<<buf<<'\n';
            data.append(buf.data(), static_cast<size_t>(r));
        }
        ::close(rfd);
        // very simple replay: parse by lines; expect only SET/DEL/EXPIRE subset
        // For robustness, reuse RespParser (not included here to avoid dependency circle). Minimal parse:
        size_t pos = 0;
        auto readLine = [&](std::string &out) -> bool
        {
            size_t e = data.find("\r\n", pos);
            if (e == std::string::npos)
                return false;
            out.assign(data.data() + pos, e - pos);
            pos = e + 2;
            return true;
        };
        while (pos < data.size())
        {
            if (data[pos++] != '*')
                break;
            std::string line;
            if (!readLine(line))
                break;
            int n = std::stoi(line);
            std::vector<std::string> parts;
            parts.reserve(n);
            for (int i = 0; i < n; ++i)
            {
                if (data[pos++] != '$')
                {
                    err = "bad bulk";
                    return false;
                }
                if (!readLine(line))
                {
                    err = "bad bulk len";
                    return false;
                }
                int len = std::stoi(line);
                if (pos + static_cast<size_t>(len) + 2 > data.size())
                {
                    err = "trunc";
                    return false;
                }
                parts.emplace_back(data.data() + pos, static_cast<size_t>(len));
                pos += static_cast<size_t>(len) + 2; // skip CRLF
            }
            if (parts.empty())
                continue;
            std::string cmd;
            cmd.reserve(parts[0].size());
            for (char c : parts[0])
                cmd.push_back(static_cast<char>(::toupper(c)));
            if (cmd == "SET" && parts.size() == 3)
            {
                store.set(parts[1], parts[2]);
            }
            else if (cmd == "DEL" && parts.size() >= 2)
            {
                std::vector<std::string> keys(parts.begin() + 1, parts.end());
                store.del(keys);
            }
            else if (cmd == "EXPIRE" && parts.size() == 3)
            {
                int64_t sec = std::stoll(parts[2]);
                store.expire(parts[1], sec);
            }
            else if (cmd == "FLUSHALL" && parts.size() == 1)
            {
                store.clearAll();
            }
            else if (cmd == "HSET" && parts.size() >= 4 && !(parts.size() % 2))
            {
                std::vector<std::string> args;
                for (int i = 2; i < parts.size(); i++)
                    args.push_back(parts[i]);
                store.hset(parts[1], args);
            }
            else if (cmd == "HDEL" && parts.size() >= 3)
            {
                std::vector<std::string> fs;
                for (size_t i = 2; i < parts.size(); ++i)
                    fs.emplace_back(parts[i]);
                store.hdel(parts[1], fs);
            }
            else if (cmd == "ZADD" && parts.size() >= 4 && !(parts.size() % 2))
            {
                std::vector<std::pair<double, std::string>> args;
                for (int i = 2; i < parts.size(); i += 2)
                {
                    double sc = std::stod(parts[i]);
                    std::string member = parts[i + 1];
                    args.push_back({sc, member});
                }
                store.zadd(parts[1], args);
            }
            else if (cmd == "ZREM" &&parts.size() >= 3)
            {
                std::vector<std::string> ms;
                for (size_t i = 2; i < parts.size(); ++i)
                    ms.emplace_back(parts[i]);
                store.zrem(parts[1], ms);
            }
        }
        return true;
    }
    std::string toRespArray(const std::vector<std::string> &command)
    {
        std::string out{};
        out.reserve(16 * command.size());
        out.append("*").append(std::to_string(command.size())).append("\r\n");
        for (const auto &s : command)
        {
            out.append("$").append(std::to_string(s.size())).append("\r\n").append(s).append("\r\n");
        }
        return out;
    }
    // 会对_pendBytes，_seqGenerator，_queue，_increaseCmds进行写操作，这是由主线程在handleCommand时调用的
    bool AofLogger::appendRaw(const std::string &raw)
    {
        //std::cout << "enter appendRaw\n";
        if (!_opt._enabled || _fd < 0)
            return true;
        std::string lineCopy{};
        bool needIncrease = _rewriting.load();
        if (needIncrease)
            lineCopy = raw;
        int64_t seq = 0;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _pendBytes += raw.size();
            seq = ++_seqGenerator;
            _queue.push_back(AofItem{std::string{raw}, seq});
        }
        // 如果正在rewrite,则不仅要将命令放入_queue中，还需要将命令塞入_increaseCmds中
        if (needIncrease)
        {
            std::lock_guard<std::mutex> lock{_increaseMutex};
            _increaseCmds.emplace_back(std::move(lineCopy));
        }
        _cv.notify_one();
        if (_opt._mode == AofMode::Always)
        {
            // 等待writeThread线程刷盘数据后通知_cvCommit才继续往下执行然后返回，如果数据还没有确保刷盘，那么需要阻塞在条件变量这里
            // 而这里的_lastSyncSqe>=sqe这个条件说的是上一次落盘命令中最大sqe命令都已经落盘了，那么比其小的自然也已经落盘了，所以可以继续执行
            std::unique_lock<std::mutex> lock{_mutex};
            _cvCommit.wait(lock, [&]
                           { return _lastSyncSqe >= seq || _stop.load(); });
        }
        //std::cout << "appendRaw queue size:" << _queue.size() << '\n';
        return true;
    }
    // 会对_pendBytes，_seqGenerator，_queue，_increaseCmds进行写操作，这是由主线程在handleCommand时调用的
    bool AofLogger::appendCommand(const std::vector<std::string> &command)
    {
        if (!_opt._enabled || _fd < 0)
            return true;
        std::string line = toRespArray(command);
        std::string lineCopy{};
        bool needIncrease = _rewriting.load();
        if (needIncrease)
            lineCopy = line;
        int64_t seq = 0;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _pendBytes += line.size();
            seq = ++_seqGenerator;
            _queue.push_back(AofItem{std::move(line), seq});
        }
        if (needIncrease)
        {
            std::lock_guard<std::mutex> lock{_increaseMutex};
            _increaseCmds.emplace_back(std::move(lineCopy));
        }
        _cv.notify_one();
        if (_opt._mode == AofMode::Always)
        {
            std::unique_lock<std::mutex> lock{_mutex};
            _cvCommit.wait(lock, [&]
                           { return _lastSyncSqe >= seq || _stop.load(); });
        }
        return true;
    }
    static std::string joinPath(const std::string &dir, const std::string &fileName)
    {
        if (dir.empty())
            return fileName;
        if (dir.back() == '/')
            return dir + fileName;
        return dir + "/" + fileName;
    }
    static bool writeAllFd(int fd, const char *data, size_t len)
    {
        // write函数并不保证一次性写入len个字节数据，所以需要根据返回值循环写入
        size_t off = 0;
        while (off < len)
        {
            ssize_t w = ::write(fd, data + off, len - off);
            if (w > 0)
            {
                off += static_cast<size_t>(w);
                continue;
            }
            if (w < 0 && (errno == EINTR || errno == EAGAIN))
                continue;
            return false;
        }
        return true;
    }
    // 新建一个临时文件，先整体写入新的命令操作，然后对这个临时文件原子地重命名，那么就会覆盖原文件，这样做的目的是保证数据一致性，那么任何时刻，这个aof要么是原来版本，要么是新版本，不会出现部分原来版本部分新版本的情况
    void AofLogger::rewriteLoop(KeyValueStore *store)
    {
        // 先生成一个临时文件路径
        std::string tmpPath = joinPath(_opt._dir, _opt._filename + ".rewrite.tmp");
        int wfd = ::open(tmpPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (wfd < 0)
        {
            //_rewriting标识rewrite操作是否正在进行中，在调用rewriteLoop这个函数需要将_rewriting原子设置为true，在结束rewriteLoop函数时需要原子地将_rewriting设置为false
            _rewriting.store(false);
            return;
        }
        // 遍历快照重组命令
        std::vector<std::pair<std::string, ValueRecord>> snap = store->snapshot();
        std::vector<std::pair<std::string, HashRecord>> hsnap = store->snapshotHash();
        std::vector<KeyValueStore::ZsetFlat> zsnap = store->snapshotZset();
        // string
        {

            for (const auto &p : snap)
            {
                const std::string &key = p.first;
                const ValueRecord &record = p.second;
                std::vector<std::string> parts = {"SET", key, record._value};
                std::string line = toRespArray(parts);
                writeAllFd(wfd, line.c_str(), line.size());
                if (record._expireAtMs > 0)
                {
                    // 说明当前的这条记录会过期
                    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
                    int64_t ttl = (record._expireAtMs - now) / 1000;
                    if (ttl < 1)
                        ttl = 1;
                    std::vector<std::string> e = {"EXPIRE", key, std::to_string(ttl)};
                    std::string el = toRespArray(e);
                    writeAllFd(wfd, el.c_str(), el.size());
                }
            }
        }

        // hash
        {

            for (const auto &p : hsnap)
            {
                // 这里内存可能撑不住
                const std::string &key = p.first;
                const HashRecord &record = p.second;
                std::vector<std::string> parts;
                parts.reserve(record._hashTable.size() * 2 + 2);
                parts.emplace_back("HSET");
                parts.emplace_back(key);
                for (const auto &[k, v] : record._hashTable)
                {
                    parts.emplace_back(k);
                    parts.emplace_back(v);
                }
                std::string line = toRespArray(parts);
                writeAllFd(wfd, line.c_str(), line.size());
                if (record._expireAtMs > 0)
                {
                    // 说明当前的这条记录会过期
                    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
                    int64_t ttl = (record._expireAtMs - now) / 1000;
                    if (ttl < 1)
                        ttl = 1;
                    std::vector<std::string> e = {"EXPIRE", key, std::to_string(ttl)};
                    std::string el = toRespArray(e);
                    writeAllFd(wfd, el.c_str(), el.size());
                }
            }
        }

        // zset
        {

            for (const auto &flat : zsnap)
            {
                for (const auto &it : flat._value)
                {
                    std::vector<std::string> parts{"ZADD", flat._key, std::to_string(it.first), it.second};
                    std::string line = toRespArray(parts);
                    writeAllFd(wfd, line.c_str(), line.size());
                }
                if (flat._expireAtMs > 0)
                {
                    // 说明当前的这条记录会过期
                    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
                    int64_t ttl = (flat._expireAtMs - now) / 1000;
                    if (ttl < 1)
                        ttl = 1;
                    std::vector<std::string> e = {"EXPIRE", flat._key, std::to_string(ttl)};
                    std::string el = toRespArray(e);
                    writeAllFd(wfd, el.c_str(), el.size());
                }
            }
        }
        // 在将新的命令写入临时文件后

        // 暂停writer
        // 目的：1、防止数据误写入新文件 - 保证新文件的纯净性
        // 2、避免写入已关闭的文件描述符 - 防止EBADF错误和数据丢失
        // 3、维护文件系统元数据一致性 - 确保目录同步的正确性
        _pauseWrite.store(true);
        {
            std::unique_lock<std::mutex> lock(_pauseMutex);
            _pauseCv.wait(lock, [&]
                          { return _writeIsPaused; });
        }
        {
            std::lock_guard<std::mutex> lock(_increaseMutex);
            for (const auto &s : _increaseCmds)
            {
                writeAllFd(wfd, s.c_str(), s.size());
            }
            _increaseCmds.clear();
        }
        ::fdatasync(wfd);
        {
            std::string finalPath = path();
            ::close(_fd);
            ::close(wfd);
            // 更改tmpPath这个文件的文件路径成finalPath这个文件路径,这是原子的文件重命名函数
            ::rename(tmpPath.c_str(), finalPath.c_str());
            // 打开新的文件路径，并且更新_fd为这个新文件路径的文件描述符
            _fd = ::open(finalPath.c_str(), O_CREAT | O_APPEND | O_WRONLY, 0644);
            // 这里打开目录，为了后续的刷盘持久化，因为之前有对目录下面的目录项进行更新，所以有必要同步目录的元数据
            int dfd = ::open(_opt._dir.c_str(), O_RDONLY);
            if (dfd > 0)
            {
                ::fsync(dfd);
                ::close(dfd);
            }
        }
        // 继续writer
        _pauseWrite.store(false);
        _pauseCv.notify_all();

        {
            std::lock_guard<std::mutex> lock(_increaseMutex);
            _increaseCmds.clear();
        }
        _rewriting.store(false);
    }
    // 得到opt中设置的dir和filname的拼接路径
    std::string AofLogger::path() const
    {
        return joinPath(_opt._dir, _opt._filename);
    }

    // 先判断_rewriting这个bool类型的原子变量是不是false,如果是那么说明还没有启动rewrite操作，那么我们就需要启动rewrite操作，将_rewriting写为true并且创建一个rewrite线程执行rewrite操作
    bool AofLogger::bgRewrite(KeyValueStore &store, std::string &err)
    {
        if (!_opt._enabled)
        {
            err = "aof disabled";
            return false;
        }
        // 如果_rewriting当前值是false才会返回true并且将_rewriting的值设置为true
        // 如果_rewriting当前值是true，那么返回false，说明_rewriting已经是true，aof的rewiting已经开始了
        bool expected = false;
        if (!_rewriting.compare_exchange_strong(expected, true))
        {
            err = "rewiting already running";
            return false;
        }
        // 创建出rewrite线程执行rewrite操作
        _rewiteThread = std::thread{&AofLogger::rewriteLoop, this, &store};
        return true;
    }
}