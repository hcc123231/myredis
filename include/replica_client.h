#pragma once
#include"config.h"
#include<thread>
namespace myredis{
    class ReplicaClient{
    public:
        explicit ReplicaClient(const ServerConfig& conf);
        ~ReplicaClient();
        void start();
        void stop();
    private:
        void threadMain();
    private:
        const ServerConfig& _conf;
        std::thread _thread;
        bool _running=false;
        int64_t _lastOffset=-1;
    };
}

