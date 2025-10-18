#pragma once
#include<cstddef>
#include<iostream>
#include<mutex>
namespace myredis{
    #define MAX_CHUNKS 1024
    #define MEM_SIZE 4096
    #define MAX_FREE_NODE_COUNT 32
    //首先思考我们的内存池如何配合我们的myredis项目来设计内存池分配
    //我们要知道myredis中会有各种长度的内存申请且没有固定大小，那么我们可以强行预设多个桶，然后采用就近原则分配
    //但是我们也可以按需分配这样就不会出现内存池内部碎片，但是新的问题就是不会重复利用内存块，那么迎刃而解的方式就是统计每一个大块内存的可用小块个数，如果可用个数为零就可以free整块内存

    struct MemChunk{
        MemChunk():_prev{nullptr},_next{nullptr},_availCntBytes{MEM_SIZE}{
            _mem=::malloc(MEM_SIZE);
            _ptr=_mem;
        }
        struct MemChunk* _prev;
        struct MemChunk* _next;
        void* _mem;//大块内存的起始地址
        void* _ptr;//大块内存目前分配到的位置
        int _availCntBytes;//可用字节数，如果可用字节数刚好等于MAX_MEM说明该块已经全部归还了，那么这块就可以视为新的可用块
    };
    class MemoryPool{
    public:
        static MemoryPool* getInstance(){
            if(pool==nullptr){
                MemoryPool* m=(MemoryPool*)malloc(sizeof(MemoryPool));
                pool=new (m) MemoryPool{};
            }
            return pool;
        }
        ~MemoryPool();
        void* allocate(size_t size);
        void deallocate(void* ptr);
    private:
        MemoryPool();
        static MemoryPool* pool;
        struct MemChunk* _head;//使用中链表表头节点
        struct MemChunk* _freeHead;//空闲块链表的表头节点
        unsigned short _freeNodeCnt;//空闲链表节点个数
        std::mutex _mtx;
    };
    
}

void* operator new(size_t size);
void operator delete(void* ptr);