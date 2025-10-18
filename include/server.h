#pragma once
#include"config.h"
namespace myredis{
    class Server{
    public:
        explicit Server(const ServerConfig& config);
        ~Server();
        int run();
    private:
        int setupListen();//设置监听
        int setupEpoll();//设置epoll
        int loop();//开始循环监听
    private:
        const ServerConfig& _config;
        int _epollFd=-1;
        int _listenFd=-1;
        int _timerFd=-1;//定时执行aof持久化(maybe)
    };
}