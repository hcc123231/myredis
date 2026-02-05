#include "../include/kv.h"
#include <chrono>
#include <algorithm>
#include<iostream>
#include <random>
namespace myredis
{
    Skiplist::Skiplist() : _head{new SkiplistNode{kMaxLevel, 0.0, ""}}, _level{1}, _length{0} {}
    void Skiplist::toVector(std::vector<std::pair<double, std::string>> &out) const
    {
        out.clear();
        out.reserve(_length);
        SkiplistNode *p = _head->_forward[0];
        while (p)
        {
            out.emplace_back(p->_score, p->_member);
            p = p->_forward[0];
        }
    }
    std::vector<std::string> KeyValueStore::listKeys() const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<std::string> out;
        out.reserve(_map.size() + _hmap.size() + _zmap.size());
        for (const auto &[k, v] : _map)
            out.push_back(k);
        for (const auto &[k, v] : _hmap)
            out.push_back(k);
        for (const auto &[k, v] : _zmap)
            out.push_back(k);
        // 排序去重
        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
        return out;
    }
    int64_t KeyValueStore::ttl(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        int64_t now = nowMs();
        cleanIfExpired(key, now);
        auto it = _map.find(key);
        if (it == _map.end())
            return -2; // key does not exist
        if (it->second._expireAtMs < 0)
            return -1; // no expire
        int64_t ms_left = it->second._expireAtMs - now;
        if (ms_left <= 0)
            return -2;
        return ms_left / 1000; // seconds (floor)
    }
    // 设置key的time to live剩余存活时间
    bool KeyValueStore::expire(const std::string &key, int64_t ttlSec)
    {
        // 这个expire命令只针对string类型值
        std::lock_guard<std::mutex> lock(_mutex);
        // 如果key值不存在或者已过期，那么不做任何操作
        int64_t now = nowMs();
        cleanIfExpired(key, now);
        auto it = _map.find(key);
        if (it == _map.end())
            return false;
        // 如果key存在且未过期，那么更新key的过期时间
        if (ttlSec < 0)
        {
            it->second._expireAtMs = -1;
            _expireIndex.erase(key);
            return true;
        }
        it->second._expireAtMs = now + 1000 * ttlSec;
        _expireIndex[key] = it->second._expireAtMs;
        return true;
    }
    //如果字符串类型的map没有存在key-value记录，那么直接在map中插入新的记录，如果已经存在则更新
    bool KeyValueStore::setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expireAtMs)
    {
        std::lock_guard<std::mutex> lk(_mutex);
        _map[key] = ValueRecord{value, expireAtMs};
        if (expireAtMs >= 0)
        {
            _expireIndex[key] = expireAtMs;
        }
        return true;
    }
    int64_t KeyValueStore::nowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    bool KeyValueStore::isExpired(const ValueRecord &v, int64_t nowMs)
    {
        return v._expireAtMs >= 0 && v._expireAtMs <= nowMs;
    }
    bool KeyValueStore::isExpired(const HashRecord &v, int64_t nowMs)
    {
        return v._expireAtMs >= 0 && v._expireAtMs <= nowMs;
    }
    bool KeyValueStore::isExpired(const ZsetRecord &v, int64_t nowMs)
    {
        return v._expireAtMs >= 0 && v._expireAtMs <= nowMs;
    }
    void KeyValueStore::cleanIfExpired(const std::string &key, int64_t nowMs)
    {
        auto it = _map.find(key);
        if (it == _map.end())
            return;
        if (isExpired(it->second, nowMs))
        {
            _map.erase(key);
            _expireIndex.erase(key);
        }
    }
    void KeyValueStore::cleanIfExpiredHash(const std::string &key, int64_t nowMs)
    {
        auto it = _hmap.find(key);
        if (it == _hmap.end())
            return;
        if (isExpired(it->second, nowMs))
        {
            _hmap.erase(key);
            _expireIndex.erase(key);
        }
    }
    void KeyValueStore::cleanIfExpiredZset(const std::string &key, int64_t nowMs)
    {
        auto it = _zmap.find(key);
        if (it == _zmap.end())
            return;
        if (isExpired(it->second, nowMs))
        {
            _zmap.erase(key);
            _expireIndex.erase(key);
        }
    }
    std::optional<std::string> KeyValueStore::get(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpired(key, nowMs()); // 在获取值之前检查是否过期，过期就执行删除操作
        auto it = _map.find(key);
        if (it != _map.end())
            return it->second._value;
        return std::nullopt;
    }
    // 全表扫描移除过期key
    int KeyValueStore::expireScanStep(int maxStep)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (maxStep <= 0 || _expireIndex.empty())
            return 0;
        int64_t now = nowMs();
        int removed = 0;
        auto it = _expireIndex.begin();
        // 迭代器随机移动，第二个参数为+向前移动，为-向后移动
        std::advance(it, static_cast<long>(std::rand() % _expireIndex.size()));
        for (int i = 0; i < maxStep && !_expireIndex.empty(); i++)
        {
            if (it == _expireIndex.end())
                it = _expireIndex.begin();
            const std::string key = it->first;
            int64_t when = it->second;
            if (when >= 0 && now >= when)
            {
                // 如果这个过期时间正确且已过期,移除所有map中该key值
                // 按key值删除即便不存在该key也是安全的，返回0
                _map.erase(key);
                _hmap.erase(key);
                _zmap.erase(key);
                // 按iterator删除，必须要iterator有效，返回删除位置下一个迭代器
                it = _expireIndex.erase(it);
                ++removed;
            }
            else
            {
                ++it;
            }
        }
        return removed;
    }
    void KeyValueStore::clearAll()
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _map.clear();
        _hmap.clear();
        _zmap.clear();
        _expireIndex.clear();
    }
    bool KeyValueStore::exists(const std::string &key)
    {
        // 这个exists命令是string类型值专用
        std::lock_guard<std::mutex> lock(_mutex);
        int64_t now = nowMs();
        cleanIfExpired(key, now);
        return _map.find(key) != _map.end();
    }
    int KeyValueStore::del(const std::vector<std::string> &keys)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        int removed = 0;
        int64_t now = nowMs();
        for (const auto &k : keys)
        {
            auto it = _map.find(k);
            if (it != _map.end())
            {
                _map.erase(k);
                _expireIndex.erase(k);
                ++removed;
            }
        }
        return removed;
    }
    std::vector<std::pair<std::string, ValueRecord>> KeyValueStore::snapshot() const
    {
        std::lock_guard<std::mutex> lock{_mutex};
        std::vector<std::pair<std::string, ValueRecord>> out;
        out.reserve(_map.size());
        for (const auto &[k, v] : _map)
        {
            out.emplace_back(k, v);
        }
        // c++17即以后，返回局部对象可以做到零成本，函数调用的接收方的内存与这个函数的返回值的内存在编译阶段就是同一块内存，所以没有任何的拷贝和移动操作
        return out;
    }
    std::vector<std::pair<std::string, HashRecord>> KeyValueStore::snapshotHash() const
    {
        std::lock_guard<std::mutex> lock{_mutex};
        std::vector<std::pair<std::string, HashRecord>> out;
        out.reserve(_hmap.size());
        for (const auto &[k, v] : _hmap)
        {
            out.emplace_back(k, v);
        }
        return out;
    }
    std::vector<KeyValueStore::ZsetFlat> KeyValueStore::snapshotZset() const
    {
        std::lock_guard<std::mutex> lock{_mutex};
        std::vector<ZsetFlat> out;
        out.reserve(_zmap.size());
        for (const auto &[k, v] : _zmap)
        {
            ZsetFlat flat;
            flat._key = k;
            flat._expireAtMs = v._expireAtMs;
            if (!v._useSkipList)
                flat._value = v._items;
            else
                v._skiplist->toVector(flat._value);
            out.emplace_back(std::move(flat));
        }
        return out;
    }
    bool KeyValueStore::set(const std::string &key, const std::string &value, std::optional<int64_t> ttlMs)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        int64_t expireAt = -1;
        if (ttlMs.has_value())
        {
            expireAt = nowMs() + *ttlMs;
        }
        _map[key] = ValueRecord{value, expireAt};
        if (expireAt >= 0)
            _expireIndex[key] = expireAt;
        else
            _expireIndex.erase(key);
        return true;
    }
    //这里的hset不会设置过期时间
    int KeyValueStore::hset(const std::string &key, const std::vector<std::string> &vec)
    {
        //std::cout<<"enter hset\n";
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredHash(key, nowMs());
        //std::cout<<"expirehash complete\n";
        // 如果key存在则返回value的引用，如果key不存在则unordered_map默认构造一个value然后返回value的引用
        HashRecord &record = _hmap[key];
        int cnt = 0;
        //std::cout<<"size:"<<vec.size()<<'\n';
        for (size_t i = 0; i < vec.size(); i += 2)
        {
            //std::cout<<"loop i:"<<i<<'\n';
            // 如果在_hashTable中找不到对应的key，那么插入这条key,value信息
            auto it = record._hashTable.find(vec[i]);
            if (it == record._hashTable.end())
            {
                //std::cout<<"can not find\n";
                record._hashTable.insert(std::make_pair(vec[i], vec[i + 1]));
                cnt++;
                continue;
            }
            // 如果找到了key，那么更新即可
            it->second = vec[i + 1];
            //<<"loop over i:"<<i<<'\n';
        }
        //std::cout<<"out hset\n";
        return cnt;
    }
    std::optional<std::string> KeyValueStore::hget(const std::string &key, const std::string &field)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredHash(key, nowMs());
        auto it = _hmap.find(key);
        if (it == _hmap.end())
            return std::nullopt;
        auto itf = it->second._hashTable.find(field);
        if (itf == it->second._hashTable.end())
            return std::nullopt;
        return itf->second;
    }
    int KeyValueStore::hdel(const std::string &key, const std::vector<std::string> &fields)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredHash(key, nowMs());
        auto it = _hmap.find(key);
        if (it == _hmap.end())
            return 0;
        int removed = 0;
        for (const auto &f : fields)
        {
            auto itf = it->second._hashTable.find(f);
            if (itf != it->second._hashTable.end())
            {
                removed += it->second._hashTable.erase(itf->first);
            }
        }
        // 如果删除完hashTable中的key-value键值对之后发现这张表都空了，那么在hmap中也需要删除掉这个hashRecord
        if (it->second._hashTable.empty())
            _hmap.erase(it);
        return removed;
    }
    bool KeyValueStore::hexists(const std::string &key, const std::string &field)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredHash(key, nowMs());
        auto it = _hmap.find(key);
        if (it == _hmap.end())
            return false;
        return it->second._hashTable.find(field) != it->second._hashTable.end();
    }
    std::vector<std::string> KeyValueStore::hgetAll(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredHash(key, nowMs());
        std::vector<std::string> out;
        auto it = _hmap.find(key);
        if (it == _hmap.end())
            return out;
        out.reserve(2 * it->second._hashTable.size());
        for (const auto &[k, v] : it->second._hashTable)
        {
            out.push_back(k);
            out.push_back(v);
        }
        return out;
    }
    int KeyValueStore::hlen(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredHash(key, nowMs());
        auto it = _hmap.find(key);
        if (it == _hmap.end())
            return 0;
        return static_cast<int>(it->second._hashTable.size());
    }
    static inline bool lessScoreMember(double aSc, const std::string &aMem, double bSc, const std::string &bMem)
    {
        if (aSc != bSc)
            return aSc < bSc;
        return aMem < bMem;
    }
    int Skiplist::randLevel()
    {
        int level = 1;
        // 生成随机数的低16位保留，然后去和一开始设定的概率值*0xFFFF比较
        while ((std::rand() & 0xFFFF) < static_cast<int>(kProbability * 0xFFFF) && level < kMaxLevel)
        {
            level++;
        }
        return level;
    }

    bool Skiplist::insert(double score, const std::string &member)
    {
        std::vector<SkiplistNode *> update{static_cast<size_t>(kMaxLevel)}; // 为vector数组预分配kMaxLevel个格子
        SkiplistNode *x = _head;                                            // 复制一份当前_head指针，接下来当作游标指针使用
        // 从当前最顶层开始遍历寻找，直到找到第0层的插入位置，并且将每一层的最后一个小于{score,member}的节点记录到update数组中，以备后续的该节点更新指针使用
        for (int i = _level - 1; i >= 0; --i)
        {
            while (x->_forward[static_cast<size_t>(i)] && lessScoreMember(x->_forward[static_cast<size_t>(i)]->_score, x->_forward[static_cast<size_t>(i)]->_member, score, member))
            {
                x = x->_forward[static_cast<size_t>(i)];
            }
            update[static_cast<size_t>(i)] = x;
        }
        // 只有当x的当前层的forward[i]大于插入节点才会停止x=x->_forward[static_cast<size_t>(i)]这一步也就是停止该层下一跳，所以x就是第0层最后一个比插入节点小的节点
        x = x->_forward[0]; // x从最后一个比插入节点小的节点变为第一个比插入节点大的节点（在第0层）
        if (x && x->_score == score && x->_member == member)
        {
            // 由于之前只是判断x节点小于插入节点就更新x，只过滤了小于条件，如果插入节点和x的下一跳节点完全相同，那么在zset中两个完全相同的节点可以视作同一个节点，所以忽略插入节点
            return false;
        }
        int level = randLevel(); // 得到插入节点的随机层数
        //_head节点和其他普通节点的结构是一样的，区别就是没有数据部分，但是指针数组还是有的，所以_head节点可以作为每一层的头节点
        if (level > _level)
        {
            // 如果插入节点的层数比原跳表的层数还要高,那么更新跳表的最高层数
            for (int i = _level; i < level; i++)
                update[static_cast<size_t>(i)] = _head; // 将高于_level层的所有层的update数组全部填充_head指针
            _level = level;
        }
        // 构造出插入节点
        SkiplistNode *next = new SkiplistNode(level, score, member);
        // 从第0层开始到最高层更新插入节点的前节点的下一跳和插入节点的下一跳
        for (int i = 0; i < level; i++)
        {
            next->_forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->_forward[static_cast<size_t>(i)];
            update[static_cast<size_t>(i)]->_forward[static_cast<size_t>(i)] = next;
        }
        ++_length;
        return true;
    }
    bool Skiplist::erase(double score, const std::string &member)
    {
        // 从上往下找到第0层的目标节点并且在这个过程中记录下每一层最后一个比目标节点小的节点，然后更新这些节点的下一跳指针就行了
        std::vector<SkiplistNode *> update{static_cast<size_t>(kMaxLevel)}; // 为vector数组预分配kMaxLevel个格子
        SkiplistNode *x = _head;                                            // 复制一份当前_head指针，接下来当作游标指针使用
        // 从当前最顶层开始遍历寻找，直到找到第0层的插入位置，并且将每一层的最后一个小于{score,member}的节点记录到update数组中，以备后续的该节点更新指针使用
        for (int i = _level - 1; i >= 0; --i)
        {
            while (x->_forward[static_cast<size_t>(i)] && lessScoreMember(x->_forward[static_cast<size_t>(i)]->_score, x->_forward[static_cast<size_t>(i)]->_member, score, member))
            {
                x = x->_forward[static_cast<size_t>(i)];
            }
            update[static_cast<size_t>(i)] = x;
        }
        x = x->_forward[0];
        // 找到要删除位置但是如果数据对不上就会放弃删除
        if (!x || x->_member != member || x->_score != score)
            return false;
        for (int i = 0; i < _level; i++)
        {
            // 这里的判断是因为我们从0到当前最高层_level循环给update中存储的节点的下一跳指针赋值，但是我们找到的要删除的这个节点的层数可能小于_level，所以会有一部分层是不用更新update存储指针的下一跳指针的
            if (update[static_cast<size_t>(i)]->_forward[static_cast<size_t>(i)] == x)
            {
                update[static_cast<size_t>(i)]->_forward[static_cast<size_t>(i)] = x->_forward[static_cast<size_t>(i)];
            }
        }
        delete x; // 利用完x给update中指针更新其下一跳之后就释放堆空间
        // 接下来就更新skiplist中的_level,_length字段
        //_level是层数字段，但是forward[]下标是从0到_level-1的，所以这里使用_level-1作为数组索引
        while (_level > 1 && _head->_forward[static_cast<size_t>(_level - 1)] != nullptr)
            --_level;
        --_length;
        return true;
    }
    // 将skiplist的start,stop范围内的元素放入out数组中
    void Skiplist::rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const
    {
        if (_length == 0)
            return;
        int64_t n = static_cast<int64_t>(_length);
        // 将客户传来的start和stop这两个数转成正确的下标值
        auto norm = [&](int64_t idx)
        {
            if (idx < 0)
                idx = n + idx;
            if (idx < 0)
                idx = 0;
            if (idx >= n)
                idx = n - 1;
            return idx;
        };
        int64_t s = norm(start);
        int64_t e = norm(stop);
        if (s > e)
            return;
        SkiplistNode *x = _head->_forward[0];
        int64_t rank = 0;
        while (x && rank < s)
        {
            x = x->_forward[0];
            ++rank;
        }
        // 来到第start个元素位置,开始将start到stop的元素全部放入out这个数组中
        while (x && rank <= e)
        {
            out.push_back(x->_member);
            x = x->_forward[0];
            ++rank;
        }
    }
    int KeyValueStore::zaddBasic(ZsetRecord &record, double score, const std::string &member)
    {
        auto mit = record._memberToScore.find(member);
        // ZsetRecord的_memberToScore中没有找到member这个key,那么就是插入新的score，member
        if (mit == record._memberToScore.end())
        {
            // 先判断ZsetRecord使用的是哪一种容器
            // 如果不用跳表数据结构作为底层容器，而是vector<std::pair<double, std::string>>作为底层容器，那么
            if (!record._useSkipList)
            {
                auto &vec = record._items;
                auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member), [](const auto &a, const auto &b)
                                           {
                    if(a.first!=b.first)return a.first<b.first;
                    return a.second<b.second; });
                vec.insert(it, std::make_pair(score, member));
                if (vec.size() > kZsetVectorPeak)
                {
                    record._useSkipList = true;
                    record._skiplist = std::make_unique<Skiplist>(); // make_unique这是一个模板函数
                    // 将vector中的数据转移到skiplist中
                    for (const auto &[s, m] : vec)
                        record._skiplist->insert(s, m);
                    // 数据全部从items中转移到skiplist,那么就需要将原来的items的数据全部销毁
                    // 这里是通过构造一个临时变量与items交换，这样items的数据就清空了并且内存也被释放了
                    std::vector<std::pair<double, std::string>>().swap(record._items);
                }
            }
            else
            {
                // 使用跳表数据结构存储元素
                record._skiplist->insert(score, member);
            }
            record._memberToScore.emplace(member, score);
            return 1;
        }
        else
        {
            // ZsetRecord的_memberToScore中找到了member这条数据,那么就是更新该member的score就行了
            double oldScore = mit->second;
            // 如果说member已经存在并且score也存在那么什么也不做
            if (oldScore == score)
                return 0;
            // 那么这个更新score不是简单的改节点的score而是先删除节点再构造新节点插入
            if (!record._useSkipList)
            {
                auto &vec = record._items;
                // 找到items中的原节点删除
                for (auto it = vec.begin(); it != vec.end(); it++)
                {
                    if (it->first == oldScore && it->second == member)
                    {
                        vec.erase(it);
                        break;
                    }
                }
                auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member), [](const auto &a, const auto &b)
                                           {
                    if(a.first!=b.first)return a.first<b.first;
                    return a.second<b.second; });
                vec.insert(it, std::make_pair(score, member));
                if (vec.size() > kZsetVectorPeak)
                {
                    record._useSkipList = true;
                    record._skiplist = std::make_unique<Skiplist>(); // make_unique这是一个模板函数
                    // 将vector中的数据转移到skiplist中
                    for (const auto &[s, m] : vec)
                        record._skiplist->insert(s, m);
                    // 数据全部从items中转移到skiplist,那么就需要将原来的items的数据全部销毁
                    // 这里是通过构造一个临时变量与items交换，这样items的数据就清空了并且内存也被释放了
                    std::vector<std::pair<double, std::string>>().swap(record._items);
                }
            }
            else
            {
                // 先删除跳表中的原节点再插入新节点
                record._skiplist->erase(oldScore, member);
                record._skiplist->insert(score, member);
            }
            // 更新_memberToScore中的score
            mit->second = score;
            return 0;
        }
    }
    //当需要从磁盘中读取hmap的数据时，提供这个函数为所有原本有过期时间的key值设置过期时间
    bool KeyValueStore::setHashExpireAtMs(const std::string& key,int64_t expire){
        std::lock_guard<std::mutex> lock(_mutex);
        auto it=_hmap.find(key);
        if(it==_hmap.end())return false;
        it->second._expireAtMs=expire;
        if(expire>=0)_expireIndex[key]=expire;
        else _expireIndex.erase(key);
        return true;
    }
    //同样的，zadd在添加键值对的时候也是没有设置过期时间这个功能
    int KeyValueStore::zadd(const std::string &key, const std::vector<std::pair<double, std::string>> &args)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredZset(key, nowMs());
        auto &record = _zmap[key];
        int cnt=0;
        for (auto it = args.begin(); it != args.end(); it++)
        {
            cnt+=zaddBasic(record, it->first, it->second);
        }
        return cnt;
    }
    bool KeyValueStore::setZsetExpireAtMs(const std::string& key,int64_t expire){
        std::lock_guard<std::mutex> lock(_mutex);
        auto it=_hmap.find(key);
        if(it==_hmap.end())return false;
        it->second._expireAtMs=expire;
        if(expire>=0)_expireIndex[key]=expire;
        else _expireIndex.erase(key);
        return true;
    }
    int KeyValueStore::zrem(const std::string &key, const std::vector<std::string> &members)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredZset(key, nowMs());
        auto it = _zmap.find(key);
        if (it == _zmap.end())
            return 0;
        int removed = 0;
        for (const auto &m : members)
        {
            auto mit = it->second._memberToScore.find(m);
            if (mit == it->second._memberToScore.end())
                continue;
            double sc = mit->second;
            it->second._memberToScore.erase(mit);
            if (!it->second._useSkipList)
            {
                // 如果zset底层容器没有使用跳表,那么就是使用vector,然后对vector容器进行元素删除操作
                auto &vec = it->second._items;
                for (auto vit = vec.begin(); vit != vec.end(); vit++)
                {
                    if (vit->first == sc && vit->second == m)
                    {
                        vec.erase(vit);
                        removed++;
                        break;
                    }
                }
            }
            else
            {
                // zset底层使用的是跳表，那么就是删除跳表中的元素
                if (it->second._skiplist->erase(sc, m))
                    ++removed;
            }
        }
        // 删除完指定元素后还需要查看底层容器是否没有数据了，如果完全没有数据那么就将这个zset中的key-zrecord也删除
        if (!it->second._useSkipList)
        {
            if (it->second._items.empty())
                _zmap.erase(it);
        }
        else
        {
            if (it->second._skiplist->size() == 0)
                _zmap.erase(it);
        }
        return removed;
    }
    std::vector<std::string> KeyValueStore::zrange(const std::string &key, int64_t start, int64_t stop)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredZset(key, nowMs());
        std::vector<std::string> out;
        auto it = _zmap.find(key);
        if (it == _zmap.end())
            return out;
        if (!it->second._useSkipList)
        {
            // vector作为容器
            const auto &vec = it->second._items;
            int64_t n = static_cast<int64_t>(vec.size());
            if (n == 0)
                return out;
            auto norm = [&](int64_t idx)
            {
                if (idx < 0)
                    idx += n;
                if (idx < 0)
                    idx = 0;
                if (idx >= n)
                    idx = n - 1;
                return idx;
            };
            int64_t s = norm(start);
            int64_t e = norm(stop);
            if (s > e)
                return out;
            for (int64_t i = s; i <= e; i++)
                out.push_back(vec[static_cast<size_t>(i)].second);
        }
        else
        {
            // skiplist作为容器
            it->second._skiplist->rangeByRank(start, stop, out);
        }
        return out;
    }
    std::optional<double> KeyValueStore::zscore(const std::string &key, const std::string &member)
    {
        // 由于zsetrecord中有一个专门记录member与score的map，所以查询分数这种操作是不需要去底层查找，直接使用这个专门的map获取
        std::lock_guard<std::mutex> lock(_mutex);
        cleanIfExpiredZset(key, nowMs());
        auto it = _zmap.find(key);
        if (it == _zmap.end())
            return std::nullopt;
        auto mit = it->second._memberToScore.find(member);
        if (mit == it->second._memberToScore.end())
            return std::nullopt;
        return mit->second;
    }
}
