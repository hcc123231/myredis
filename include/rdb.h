#pragma once
#include"config.h"
#include<string>
#include"kv.h"
namespace myredis{
    //前向声明
    class KeyValueStore;
    class Rdb{
    public:
        Rdb()=default;
        explicit Rdb(const RdbOptions& opt):_opts{opt}{}
        void setOptions(const RdbOptions& opt){_opts=opt;}
        bool save(const KeyValueStore& store,std::string& err)const;
        bool load(KeyValueStore& store,std::string& err)const;
        std::string path()const;
    private:
        RdbOptions _opts{};
    };
}



