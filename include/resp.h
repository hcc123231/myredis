#pragma once
#include<string>
#include<string_view>
#include<optional>
#include<vector>
namespace myredis{
    enum RespType{
        SimpleString=0,//简单字符串
        Error,
        Integer,
        BulkString,//大量字符串
        Array,
        Null
    };

    //resp协议的数据结构
    //包含命令字符串和它的类型
    struct RespValue{
        RespType _type=RespType::Null;
        std::string _bulk;
        std::vector<RespValue> _array;
    };
    //resp协议解析器
    class RespParser{
    public:
        void append(std::string_view data);
        std::optional<std::pair<RespValue,std::string>> tryParseOneWithRaw();
        std::optional<RespValue> tryParseOne();
    private:
        bool parseLine(size_t& pos,std::string& outLine);
        bool parseSimple(size_t& pos,RespType type,RespValue& out);
        bool parseInteger(size_t& pos,int64_t& out);
        bool parseBulkString(size_t& pos,RespValue& out);
        bool parseArray(size_t& pos,RespValue& out);
    private:
        std::string _buf;//接收到的网络数据全部放在这里
    };

    //响应回发数据封装
    std::string respSimpleString(std::string_view s);
    std::string respError(std::string_view s);
    std::string respBulkString(std::string_view s);
    std::string respNullBulk();
    std::string respInteger(int64_t val);
}