#include "../include/server.h"
#include"../include/config_loader.h"
#include<csignal>
#include<iostream>
namespace myredis
{
    volatile std::sig_atomic_t gShouldStop=0;
    void handleSignal(int sigNum){
        (void)sigNum;//避免未使用参数的警告
        gShouldStop=1;//结束程序标志
        //暂时代替一下
        //std::signal(sigNum,SIG_DFL);
    }
    void printUsage(char* arg){
        std::cout<<"usage:\n"<<" "<<arg<<"[--port <port>] [--bind <ip>] [--config <file>]"<<'\n';
    }
    //解析命令行参数，如果有--config那么将文件交给loadConfigFromFile来解析
    bool parseArgs(int argc,char** argv,ServerConfig& outConfig){
        for(int i=1;i<argc;i++){
            const std::string arg=argv[i];
            if(arg=="--port"&&i+1<argc){
                outConfig._port=static_cast<uint16_t>(std::stoi(argv[++i]));
            }
            else if(arg=="--bind"&&i+1<argc){
                outConfig._bindAddr=std::string{argv[++i]};
            }
            else if(arg=="--config"&&i+1<argc){
                std::string file=argv[++i];
                std::string err;
                if(!loadConfigFromFile(file,outConfig,err)){
                    std::cerr<<err<<'\n';
                    return false;
                }
            }
            else if(arg=="-h"||arg=="-help"){
                printUsage(argv[0]);
            }
            else{
                std::cout<<"Unknown argument: "<<arg<<"\n";
                printUsage(argv[0]);
                return false;
            }
        }
        return true;
    }
    int runServer(const ServerConfig& config){
        //std::signal是注册一个信号处理函数，这样当信号被触发时就会调用对应的信号处理函数
        //std::signal(SIGINT,handleSignal);
        std::signal(SIGTERM,handleSignal);
        Server server(config);
        return server.run();
    }
}
using namespace myredis;
int main(int argc, char **argv)
{
    ServerConfig config;
    if(!parseArgs(argc,argv,config))return 1;
    return runServer(config);
}