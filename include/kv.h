#pragma once
#include <string>
#include <unordered_map>
#include<optional>
#include <memory>
#include <vector>
#include<mutex>
namespace myredis
{
    // key-value数据结构

    struct ValueRecord
    {
        std::string _value;
        int64_t _expireAtMs = -1;
    };
    struct HashRecord
    {
        std::unordered_map<std::string, std::string> _hashTable;
        int64_t _expireAtMs = -1;
    };
    struct SkiplistNode
    {
        double _score;
        std::string _member;
        std::vector<SkiplistNode *> _forward;
        SkiplistNode(int level, double sc, const std::string &mem) : _score{sc}, _member{mem}, _forward{static_cast<size_t>(level), nullptr} {}
    };
    struct Skiplist
    {
    public:
        Skiplist();
        ~Skiplist()=default;
        void toVector(std::vector<std::pair<double,std::string>>& out)const;
        bool insert(double score,const std::string& member);
        bool erase(double score,const std::string& member);
        size_t size()const{return _length;}
        void rangeByRank(int64_t start,int64_t stop,std::vector<std::string>& out)const;
    private:
        static constexpr int kMaxLevel = 32;//最大层数
        int randLevel();
        static constexpr double kProbability = 0.25;
        SkiplistNode *_head;
        int _level;//当前层数
        size_t _length;//实际数据节点不包含头节点(跳表容器)
    };

    // 这是zset的数据结构对外接口
    struct ZsetRecord
    {
        // 是否使用跳表数据结构标志位
        bool _useSkipList = false;
        std::vector<std::pair<double, std::string>> _items; // 当数据量较小时使用vector作为底层容器

        std::unique_ptr<Skiplist> _skiplist;                    // 跳表数据结构指针
        std::unordered_map<std::string, double> _memberToScore; // 根据member找到score
        int64_t _expireAtMs = -1;
    };
    class KeyValueStore{
    public:
        int expireScanStep(int maxStep);
        void clearAll();
        bool setWithExpireAtMs(const std::string& key,const std::string& value,int64_t expireAtMs);
        bool exists(const std::string& key);
        int64_t ttl(const std::string& key);
        bool expire(const std::string& key,int64_t ttlSec);
        std::vector<std::pair<std::string,ValueRecord>> snapshot()const;
        std::vector<std::pair<std::string,HashRecord>> snapshotHash()const;
        struct ZsetFlat{
            std::string _key;
            std::vector<std::pair<double,std::string>> _value;
            int64_t _expireAtMs;
        };
        std::vector<ZsetFlat> snapshotZset()const;
        std::vector<std::string> listKeys()const;
        bool set(const std::string& key,const std::string& value,std::optional<int64_t> ttlMs=std::nullopt);
        std::optional<std::string> get(const std::string& key);
        int del(const std::vector<std::string>& keys);

        //hash
        int hset(const std::string& key,const std::vector<std::string>& vec);
        std::optional<std::string> hget(const std::string& key,const std::string& field);
        int hdel(const std::string& key,const std::vector<std::string>& fields);
        bool hexists(const std::string& key,const std::string& field);
        std::vector<std::string> hgetAll(const std::string& key);
        int hlen(const std::string& key);
        bool setHashExpireAtMs(const std::string& key,int64_t expire);
        //zset
        int zadd(const std::string& key,const std::vector<std::pair<double,std::string>>& args);
        int zrem(const std::string& key,const std::vector<std::string>& members);
        std::vector<std::string> zrange(const std::string& key,int64_t start,int64_t stop);
        std::optional<double> zscore(const std::string& key,const std::string& member);
        bool setZsetExpireAtMs(const std::string& key,int64_t expire);
    private:
        int zaddBasic(ZsetRecord& record,double score,const std::string& member);
        static int64_t nowMs();
        void cleanIfExpired(const std::string& key,int64_t nowMs);
        void cleanIfExpiredHash(const std::string& key,int64_t nowMs);
        void cleanIfExpiredZset(const std::string& key,int64_t nowMs);
        static bool isExpired(const ValueRecord& v,int64_t nowMs);
        static bool isExpired(const HashRecord& v,int64_t nowMs);
        static bool isExpired(const ZsetRecord& v,int64_t nowMs);
        static constexpr size_t kZsetVectorPeak=128;//定义zset数据结构使用vector作为底层容器的最大数据容量
    private:
        std::unordered_map<std::string,ValueRecord> _map;
        std::unordered_map<std::string,ValueRecord>::iterator _scanIterator=_map.end();
        std::unordered_map<std::string,HashRecord> _hmap;
        std::unordered_map<std::string,HashRecord>::iterator _hscanIterator=_hmap.end();
        std::unordered_map<std::string,ZsetRecord> _zmap;
        std::unordered_map<std::string,ZsetRecord>::iterator _zcanIterator=_zmap.end();

        std::unordered_map<std::string,int64_t> _expireIndex;//设置key过期值，比如说要将某一个key设置为定时key，那么使用这个存储key和对应的过期时间

        mutable std::mutex _mutex;
    };

}