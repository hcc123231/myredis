#pragma once
#include"config.h"
#include<atomic>
#include<string>
#include<thread>
#include<mutex>
#include<condition_variable>
#include<deque>
#include<vector>
#include<chrono>
namespace myredis{
    std::string toRespArray(const std::vector<std::string>& command);
    class KeyValueStore;
    class AofLogger{
    public:
        AofLogger()=default;
        ~AofLogger();
        void shutdown();
        bool init(const AofOptions& opt,std::string& err);
        bool appendRaw(const std::string& raw);
        bool appendCommand(const std::vector<std::string>& command);
        bool isEnabled()const{return _opt._enabled;}
        bool bgRewrite(KeyValueStore& store,std::string& err);
        bool load(KeyValueStore& store,std::string& err);
        std::string path()const;
        AofMode mode()const{return _opt._mode;}
    private:
        int _fd=-1;//这是文件fd，对应的是aof文件的
        AofOptions _opt;
        std::atomic<bool> _running{false};
        int _timerFd=-1;
        struct AofItem{
            std::string _item;
            int64_t _seq;
        };
        std::thread _writeThread;//写aof命令单独开一个线程
        std::mutex _mutex;//全局锁，用于在主线程和_writeThread线程之间保护共享资源，这个共享资源大概是主线程往_queue中添加数据，_writeThread线程从_queue中取数据
        std::condition_variable _cv;//用于appendRaw和writeLoop之间的同步，也就是主线程和_writeThread线程之间的同步，当有数据加入到_queue中appendRaw就会通知writeLoop结束_cv的wait
        std::condition_variable _cvCommit;//用于等待写入完成的同步条件变量，appendRaw添加命令到_queue中然后通知_writeThread线程写入到文件中，写入完成后通知appendRaw这个线程结束cvCommit.wait
        std::deque<AofItem> _queue;//存储原始命令的队列
        std::atomic<bool> _stop{false};
        size_t _pendBytes=0;//待写入字节数
        std::chrono::steady_clock::time_point _lastSyncTimepoint{std::chrono::steady_clock::now()};
        std::atomic<int64_t> _seqGenerator{0};
        int64_t _lastSyncSqe{0};//上一次写入命令的序列号，就是上一批次命令的序列号中最大的序列号值

        std::atomic<bool> _rewriting{false};
        std::thread _rewiteThread;//重写aof命令单独开一个线程
        std::mutex _increaseMutex;
        std::vector<std::string> _increaseCmds;//重写期间新增的命令缓存

        std::atomic<bool> _pauseWrite{false};
        std::mutex _pauseMutex;
        std::condition_variable _pauseCv;
        bool _writeIsPaused{false};

        void writeLoop();
        void rewriteLoop(KeyValueStore* store);
    };
}