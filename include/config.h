#pragma once
#include <cstdint>
#include <string>
// 服务配置数据结构
namespace myredis
{
    // 提供给aof持久化操作的几种模式选项
    enum AofMode
    {
        No = 0,
        EverySec,
        Always
    };
    // aof持久化选项参数
    // aof持久化策略就是记录下每一条写操作命令
    struct AofOptions
    {
        bool _enabled = false;                    // 默认不开启aof持久化策略
        AofMode _mode = AofMode::Always;          // 默认模式为always,也就是每一次命令执行都会将这条命令刷到磁盘文件
        std::string _dir = "./data";              // 磁盘文件的目录
        std::string _filename = "appendonly.aof"; // 磁盘文件名

        size_t _batch_bytes = 256 * 1024;          // 每批聚合写入的目标字节数
        int _batch_wait_us = 1500;                 // 聚合等待上限（微秒）
        size_t _prealloc_bytes = 2 * 1024 ; // 初始预分配大小
        int _sync_interval_ms = 1000;              // everysec 实际同步周期（毫秒），可调平滑尾延迟
        // optional smoothing knobs (Linux only)
        bool _use_sync_file_range = false;         // 写入后触发后台回写（SFR_WRITE）
        size_t _sfr_min_bytes = 512 * 1024;        // 达到该批量再调用 sync_file_range，避免过于频繁
        bool _fadvise_dontneed_after_sync = false; // 每次 fdatasync 后对已同步范围做 DONTNEED
    };
    // rdb持久化选项参数
    // rdb持久化策略就是保存某一时刻redis全部数据的快照到磁盘文件中去
    struct RdbOptions
    {
        bool _enabled = false;
        std::string _dir = "./data";        // 磁盘文件的目录
        std::string _filename = "dump.rdb"; // 磁盘文件名
    };
    // 主从复制选项参数
    struct ReplicaOptions
    {
        bool _enabled = false;
        std::string _masterHost = ""; // 主节点ip
        uint16_t _masterPort = 0;     // 主节点port
    };

    // 服务配置类
    struct ServerConfig
    {
        uint16_t _port = 6379;             // 服务端口号
        std::string _bindAddr = "0.0.0.0"; // 绑定地址
        AofOptions _aof;
        RdbOptions _rdb;
        ReplicaOptions _replica;
    };
}
