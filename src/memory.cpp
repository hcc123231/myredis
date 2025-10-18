#include "../include/memory.h"
#include <thread>
/*
namespace myredis
{
    MemoryPool *MemoryPool::pool = nullptr;
    MemoryPool::MemoryPool() : _head{nullptr}, _freeHead{nullptr}, _freeNodeCnt{0}
    {
    }
    MemoryPool::~MemoryPool()
    {
        std::cout<<"~MemoryPool\n";
        // 释放所有使用中链表节点
        struct MemChunk *current = _head;
        while (current != nullptr)
        {
            struct MemChunk *next = current->_next;
            ::free(current->_mem);
            ::free(current);
            current = next;
        }

        // 释放所有空闲链表节点
        current = _freeHead;
        while (current != nullptr)
        {
            struct MemChunk *next = current->_next;
            ::free(current->_mem);
            ::free(current);
            current = next;
        }

        _head = nullptr;
        _freeHead = nullptr;
    }
    // 链表分为使用中链表和空闲链表，也就是说归还的大块可以入空闲链表，当使用中链表不足时需要申请新的大块内存，这时可以先检查空闲链表是否有节点可用，
    // 如果有那么可以复用空闲链表的节点并且移动到使用中链表的头节点，后续还会在deallocate时检测空闲链表是否超过节点个数阈值从而主动free掉空闲链表的多余节点
    // 分配的每一个小块头部包含长度信息，这个长度指长度本身内存长度加上要分配的内存长度
    void *MemoryPool::allocate(size_t size)
    {
        //std::lock_guard<std::mutex> lock{_mtx};
        
        std::cout << "enter allocate\n";
        if (size == 0)
            throw std::bad_alloc();
        if (size > MEM_SIZE)
            return ::malloc(size);
        // 如果头节点为null，那么新增一个块并且置为_head
        if (!_head)
        {
            void *mem = malloc(sizeof(struct MemChunk));
            if (!mem)
                throw std::bad_alloc();
            _head = new (mem) MemChunk();
        }
        // 找头节点但是没有空闲内存可使用，那么优先考虑从空闲链表中拿取节点，如果没有节点，那么就需要malloc到head上了
        if (_head && (char *)(_head->_mem) + MEM_SIZE < (char *)(_head->_ptr) + size)
        {
            if (_freeHead)
            {
                struct MemChunk *tmp = _freeHead;
                _freeHead = _freeHead->_next;
                tmp->_next = _head;
                _head->_prev = tmp;
                _head = tmp;
                tmp->_ptr = tmp->_mem;
                _freeNodeCnt--;
            }
            else
            {
                void *mem = malloc(sizeof(struct MemChunk));
                if (!mem)
                    throw std::bad_alloc();
                struct MemChunk *chunk = new (mem) MemChunk();
                chunk->_next = _head;
                _head->_prev = chunk;
                _head = chunk;
            }
        }
        // 找到头节点并且如果头节点还有空闲内存可使用，那么从头节点中切分小块内存
        if (_head && (char *)(_head->_mem) + MEM_SIZE >= (char *)(_head->_ptr) + size)
        {
            void *ptr = _head->_ptr;
            *(unsigned short *)ptr = (unsigned short)size;
            size_t sz = sizeof(unsigned short) + size;
            _head->_ptr = (void *)((char *)(_head->_ptr) + sz);
            _head->_availCntBytes -= sz;
            std::cout << "ok out allocate\n";
            return (char *)ptr + sizeof(unsigned short);
        }
        std::cout << "no ok out allocate\n";
        return nullptr;
        //return ::malloc(size);
    }


    void MemoryPool::deallocate(void *ptr)
    {
        std::cout << "enter deallocate\n";
        // 归还小块内存时先考虑遍历使用中链表，找到属于哪一大块，然后对其availCntBytes加上长度
        if(ptr==nullptr)return;
        if(!_head)throw std::runtime_error("mem deallocate error");
        struct MemChunk* adv=_head;
        bool find=false;//找到大块标志位
        do{
            //先判断当前块是否为要找的大块内存
            if((char*)(adv->_mem)+MEM_SIZE+sizeof(unsigned short)<ptr)continue;
            //先拆解出ptr隐藏的长度信息
            char* p=(char*)ptr-sizeof(unsigned short);
            unsigned short len=*(unsigned short*)p;
            //再对块的长度进行操作
            adv->_availCntBytes+=len;
            //如果空闲长度等于MEM_SIZE，那么将这个块移动到空闲链表中
            if(adv->_availCntBytes==MEM_SIZE){
                 adv->_prev->_next=adv->_next;
                 adv->_next->_prev=adv->_prev;
                 struct MemChunk* tmp=adv;
                 adv=adv->_prev;
                 tmp->_prev=nullptr;
                 tmp->_next=_freeHead;
                 _freeHead=tmp;
                 _freeNodeCnt++;
            }
            find=true;
        }while((adv=adv->_next)!=nullptr);
        // 对于特大快来说，肯定是在上面的while过程中没有能够找到，所以需要额外的free操作
        // if(!find)::free(ptr);
        // 最后再来判断空闲链表是否超限,超限就需要释放掉一部分节点
        if(_freeNodeCnt>MAX_FREE_NODE_COUNT){
            unsigned short cnt=_freeNodeCnt-MAX_FREE_NODE_COUNT;
            for(int i=0;i<cnt;i++){
                struct MemChunk* tmp=_freeHead;
                _freeHead=_freeHead->_next;
                ::free(tmp->_mem);
                ::free(tmp);
            }
        }
        std::cout << "out deallocate\n";
    }
}

long long cnt = 0;
long long ss = 0;
void *operator new(size_t size)
{
    cnt++;
    ss = ss + 2 + size;
    std::cout << "thread ID:" << std::this_thread::get_id() << '\n';
    void *ptr = myredis::MemoryPool::getInstance()->allocate(size);
    if (ptr == nullptr)
        throw std::bad_alloc();
    std::cout << "new was called size:" << size << " -- cnt:" << cnt << " " << "addr:" << ptr << "\n";
    std::cout << "current allocate size:" << ss << '\n';
    return ptr;
}
long long dcnt = 0;
void operator delete(void *ptr)
{
    dcnt++;
    std::cout << "delete was called -- Dcnt:" << dcnt << " " << "ptr:" << ptr << '\n';
    myredis::MemoryPool::getInstance()->deallocate(ptr);
}*/
void* mem = malloc(10716650);
char* ptr = (char*)mem;
char* const end = ptr + 10716650;

void* allocate(size_t size) {
    // 16字节对齐
    const size_t alignment = 16;
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    uintptr_t aligned_addr = (addr + alignment - 1) & ~(alignment - 1);
    size_t padding = aligned_addr - addr;
    
    // 边界检查
    if (ptr + padding + size > end) {
        std::cerr << "内存耗尽！需要:" << size << " 可用:" << (end - ptr) << "\n";
        throw std::bad_alloc();
    }
    
    ptr += padding;
    void* result = ptr;
    ptr += size;
    
    std::cout << "分配大小:" << size << " 对齐后:" << (size + padding) 
              << " 地址:" << result << "\n";
    return result;
}
void* operator new(size_t size) {
    void* p=allocate(size);
    std::cout << "new was called size:" << size << " " << "addr:" << p << "\n";
    return p;
}

void operator delete(void* ptr){
    
}