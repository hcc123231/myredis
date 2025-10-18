#include "../include/resp.h"
#include <charconv>
namespace myredis
{
    void RespParser::append(std::string_view data)
    {
        _buf.append(data.data(), data.length());
    }
    //得到pos开始第一个"\r\n"之前的数据并且pos要移动到"\r\n"后面一格位置
    bool RespParser::parseLine(size_t &pos, std::string &outLine)
    {
        size_t end = _buf.find("\r\n", pos);
        if (end == std::string::npos)
            return false;
        outLine.assign(_buf.data() + pos, end - pos);
        pos = end + 2;
        return true;
    }
    //得到简单字符串，简单字符串形如"+OK\r\n",通过parseLine得到这个Ok，所以这里也是已经将pos移动到"\r\n"后一格了
    bool RespParser::parseSimple(size_t &pos, RespType type, RespValue &out)
    {
        std::string s;
        if (!parseLine(pos, s))
            return false;
        out._type = type;
        out._bulk = std::move(s);
        return true;
    }
    //Integer其实和之前的简单字符串的形式很像，形如":123\r\n",那么直接调用parseLine得到123,并且也是将pos移动到"\r\n"后一格了
    bool RespParser::parseInteger(size_t &pos, int64_t &out)
    {
        std::string s;
        if (!parseLine(pos, s))
            return false;
        int64_t integer = 0;
        auto end = s.data() + s.size();
        auto [ptr, ec] = std::from_chars(s.data(), end, integer);
        if (ec != std::errc{} || ptr != end)
            return false;
        out = integer;
        return true;
    }
    //形如"$8\r\nwohenhao\r\n"就是bulkString,特点是前半段是Integer这样的格式，后半段是simpleString这样的格式，所以我们可以先调用parseInteger得到这个8,这时候pos已经移动到\
        'w'这个位置，然后直接利用pos位置加上8这个长度信息拿到这个bulkString内容，最后再移动pos到末尾的"\r\n"后一格
    bool RespParser::parseBulkString(size_t &pos, RespValue &out)
    {
        int64_t stringLen = 0;
        if (!parseInteger(pos, stringLen))
            return false;
        if (stringLen == -1)
        {
            // 说明这个字符串为NULL
            out._type = RespType::Null;
            return true;
        }
        if (stringLen < 0)
        {
            // 协议层面错误
            return false;
        }
        //_buf实际长度如果小于pos+前面的字符串长度信息+'\r\n'这个长度，那么说明对不上
        if (_buf.size() < pos + static_cast<size_t>(stringLen) + 2)
            return false;
        out._type = RespType::BulkString;
        out._bulk.assign(_buf.data() + pos, static_cast<size_t>(stringLen));
        pos += static_cast<size_t>(stringLen);
        // 如果在字符串中安插\r\n也是直接返回false
        if (!(pos + 1 < _buf.size() && _buf[pos] == '\r' && _buf[pos + 1] == '\n'))
            return false;
        pos += 2;
        return true;
    }
    //数组形如"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",开头标明数组元素个数，后面就是任意五种resp协议数据类型
    //所以先使用parseInteger得到数组长度，将pos移动到'$'处，然后开始循环拿到后面的元素
    bool RespParser::parseArray(size_t &pos, RespValue &out)
    {
        int64_t count = 0;
        if (!parseInteger(pos, count))
            return false;
        // 同样的，如果数组最开始位置的数字为-1，说明这是一个空值
        if (count == -1)
        {
            out._type = RespType::Null;
            return true;
        }
        if (count < 0)
            return false;
        out._type = RespType::Array;
        out._array.clear();
        out._array.reserve(static_cast<size_t>(count));
        for (size_t i = 0; i < count; i++)
        {
            //判断pos位置是否移出_buf
            if (pos >= _buf.size())
                return false;
            // 记录前缀
            char prefix = _buf[pos++]; 
            RespValue elem;
            bool ok = false;
            switch (prefix)
            {
            case '+':
                ok = parseSimple(pos, RespType::SimpleString, elem);
                break;
            case '-':
                ok = parseSimple(pos, RespType::Error, elem);
                break;
            case ':':
            {
                int64_t integer = 0;
                ok = parseInteger(pos, integer);
                if (ok)
                {
                    elem._type = RespType::Integer;
                    elem._bulk = std::to_string(integer);
                }
                break;
            }
            case '$':
                ok = parseBulkString(pos, elem);
                break;
            case '*':
                ok = parseArray(pos, elem);
                break;
            default:
                return false;
            }
            if(!ok)return false;
            out._array.emplace_back(std::move(elem));
        }
        return true;
    }
    std::optional<RespValue> RespParser::tryParseOne(){
        if (_buf.empty())
            return std::nullopt;
        size_t pos = 0;
        char prefix = _buf[pos++]; // 得到最前面的符号，用来区别不同命令
        RespValue out;
        bool ok = false;
        switch (prefix)
        {
        case '+':
            ok = parseSimple(pos, RespType::SimpleString, out);
            break;
        case '-':
            ok = parseSimple(pos, RespType::Error, out);
            break;
        case ':':
        {
            int64_t integer = 0;
            ok = parseInteger(pos, integer);
            if (ok)
            {
                out._type = RespType::Integer;
                out._bulk = std::to_string(integer);
            }
            break;
        }
        case '$':
            ok = parseBulkString(pos, out);
            break;
        case '*':
            ok = parseArray(pos, out);
            break;
        default:
            return std::make_optional(RespValue{RespType::Error,std::string{"protocol error"}});
        }
        if(!ok)return std::nullopt;
        _buf.erase(0,pos);
        return out;
    }
    std::optional<std::pair<RespValue, std::string>> RespParser::tryParseOneWithRaw()
    {
        if (_buf.empty())
            return std::nullopt;
        size_t pos = 0;
        char prefix = _buf[pos++]; // 得到最前面的符号，用来区别不同命令
        RespValue out;
        bool ok = false;
        switch (prefix)
        {
        case '+':
            ok = parseSimple(pos, RespType::SimpleString, out);
            break;
        case '-':
            ok = parseSimple(pos, RespType::Error, out);
            break;
        case ':':
        {
            int64_t integer = 0;
            ok = parseInteger(pos, integer);
            if (ok)
            {
                out._type = RespType::Integer;
                out._bulk = std::to_string(integer);
            }
            break;
        }
        case '$':
            ok = parseBulkString(pos, out);
            break;
        case '*':
            ok = parseArray(pos, out);
            break;
        default:
            return std::make_optional(std::make_pair(RespValue{RespType::Error,std::string{"protocol error"},{}},std::string{}));
        }
        if(!ok)return std::nullopt;
        //拿到_buf的全部数据后清空_buf
        std::string raw{_buf.data(),pos};
        _buf.erase(0,pos);
        return std::make_pair(std::move(out),std::move(raw));
    }

    std::string respSimpleString(std::string_view s){
        return std::string{"+"}+std::string{s}+std::string{"\r\n"};
    }
    std::string respError(std::string_view s){
        return std::string{"-"}+std::string{s}+std::string{"\r\n"};
    }
    std::string respBulkString(std::string_view s){
        return std::string{"$"}+std::to_string(s.length())+std::string{"\r\n"}+std::string{s}+std::string{"\r\n"};
    }
    std::string respNullBulk(){
        return "$-1\r\n";
    }
    std::string respInteger(int64_t val){
        return std::string{":"}+std::to_string(val)+std::string{"\r\n"};
    }
}
