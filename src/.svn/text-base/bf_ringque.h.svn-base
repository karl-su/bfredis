#ifndef __BF_RING_QUEUE_H__
#define __BF_RING_QUEUE_H__

/* BLOOM FILTER BLOCK 内存分配数据结构 */
//半闭半开的环形队列
//16 free blocks : 16 * 512M = 8G
#define BF_BLOCK_RING_QUEUE_SIZE (16 + 1)
struct STBlockRingQueue {
    volatile unsigned int readIndex;
    volatile unsigned int writeIndex;
    volatile sds blocks[BF_BLOCK_RING_QUEUE_SIZE];
};
static volatile struct STBlockRingQueue g_stBlockQueue = {0, 0, {0}};

static inline int BlockQueueLen()
{
    unsigned int r = g_stBlockQueue.readIndex;
    unsigned int w = g_stBlockQueue.writeIndex;
    if (w < r)
        return w + (BF_BLOCK_RING_QUEUE_SIZE - r);
    return w -r;
}

static inline int PushOBlock()
{
    unsigned int r = g_stBlockQueue.readIndex;
    unsigned int w = g_stBlockQueue.writeIndex;
    if ((r == 0 && w == BF_BLOCK_RING_QUEUE_SIZE - 1)
            || (w + 1 == r))
        return 0;
    
    int len = BlockQueueLen();
    sds s = sdsnewlen(NULL, 512 * 1024 * 1024);
    g_stBlockQueue.blocks[g_stBlockQueue.writeIndex] = s;
    if (g_stBlockQueue.writeIndex == BF_BLOCK_RING_QUEUE_SIZE - 1)
        g_stBlockQueue.writeIndex = 0;
    else
        g_stBlockQueue.writeIndex++;
    
    return len + 1;
}

static inline sds PopBlock()
{
    if (g_stBlockQueue.readIndex == g_stBlockQueue.writeIndex)
        return NULL;

    sds s = g_stBlockQueue.blocks[g_stBlockQueue.readIndex];
    if (g_stBlockQueue.readIndex == BF_BLOCK_RING_QUEUE_SIZE - 1)
       g_stBlockQueue.readIndex = 0;
    else
        g_stBlockQueue.readIndex++;
    
    return s; 
}

/* 主线程与计算线程池通信的结构体 */
//request
struct BFRequest {
    client*     client;         //客户端
    uint64_t    seq;            //客户端的请求seq
    
    uint64_t    callTime;       //开始调用时间(us)
    uint64_t    enReqQueTime;   //入请求队列时间(us)
    uint32_t    reqQueLen;      //进请求队列时,请求队列的长度

    uint32_t    method;         //方法, 1 - bfbget ; 2 - bfbset
    robj*       bitmap;
    robj*       strs;
    int         len;
    int         hashcnt;
};

//reply
struct BFReply{
    client*     client;         //客户端

    uint64_t    seq;            //客户端的请求seq
 
    uint64_t    callTime;       //开始调用时间(us)
    uint64_t    enReqQueTime;   //入请求队列时间(us)
    uint64_t    deReqQueTime;   //出请求队列时间(us)
    uint64_t    enRspQueTime;   //入响应队列时间(us)
    uint64_t    deRspQueTime;   //出响应队列时间(us)
    uint64_t    doneTime;       //结束调用时间(us)
    uint32_t    reqQueLen;      //进请求队列时,请求队列的长度
    uint32_t    rspQueLen;      //进响应队列时,响应队列的长度

    uint32_t    method;         //方法
    robj*       bitmap;
    robj*       strs;
    int         len;
    int         hashcnt;

    sds         result;
    uint32_t    existCnt;       //存在的str数
    uint32_t    noExistCnt;     //不存在的str数
};

/* 主线程与计算线程池通信的环形队列 */
//请求队列
//一写多读
#define BF_REQUEST_RING_QUEUE_SIZE (512 + 1)
struct STReqRingQueue {
    volatile unsigned int readIndex;
    volatile unsigned int writeIndex;
    volatile struct BFRequest reqs[BF_REQUEST_RING_QUEUE_SIZE];
};
static volatile struct STReqRingQueue g_stReqQueue = {0, 0, {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}};
static pthread_mutex_t mutexReqQue = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condReqQue = PTHREAD_COND_INITIALIZER;

static inline int ReqQueueLen()
{
    unsigned int r = g_stReqQueue.readIndex;
    unsigned int w = g_stReqQueue.writeIndex;
    if (w < r)
        return w + (BF_REQUEST_RING_QUEUE_SIZE - r);
    return w -r;
}

static inline int PushRequest(struct BFRequest* req)
{
    unsigned int r = g_stReqQueue.readIndex;
    unsigned int w = g_stReqQueue.writeIndex;
    if ((r == 0 && w == BF_REQUEST_RING_QUEUE_SIZE - 1)
            || (w + 1 == r))
    {
        return 0;
    }
    
    int len = ReqQueueLen();
    g_stReqQueue.reqs[g_stReqQueue.writeIndex] = *req;
    if (g_stReqQueue.writeIndex == BF_REQUEST_RING_QUEUE_SIZE - 1)
        g_stReqQueue.writeIndex = 0;
    else
        g_stReqQueue.writeIndex++;
    
    return len + 1;
}

static inline int PopRequest(struct BFRequest* req)
{
    if (g_stReqQueue.readIndex == g_stReqQueue.writeIndex)
    {
        return 0;
    }

    int len = ReqQueueLen();
    *req = g_stReqQueue.reqs[g_stReqQueue.readIndex];
    if (g_stReqQueue.readIndex == BF_REQUEST_RING_QUEUE_SIZE - 1)
       g_stReqQueue.readIndex = 0;
    else
        g_stReqQueue.readIndex++;
    
    return len; 
}

//响应队列
//多写一读
#define BF_REPLY_RING_QUEUE_SIZE (2048 + 1)
struct STReplyRingQueue {
    volatile unsigned int readIndex;
    volatile unsigned int writeIndex;
    volatile struct BFReply replys[BF_REPLY_RING_QUEUE_SIZE];
};
static volatile struct STReplyRingQueue g_stReplyQueue = {0, 0, {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}};
static pthread_mutex_t mutexReplyQue = PTHREAD_MUTEX_INITIALIZER;

static inline int ReplyQueueLen()
{
    unsigned int r = g_stReplyQueue.readIndex;
    unsigned int w = g_stReplyQueue.writeIndex;
    if (w < r)
        return w + (BF_REPLY_RING_QUEUE_SIZE - r);
    return w -r;
}

extern void notifyMainThread();
static inline int PushReply(struct BFReply* reply)
{
    if (pthread_mutex_lock(&mutexReplyQue) != 0)
        return -1;

    unsigned int r = g_stReplyQueue.readIndex;
    unsigned int w = g_stReplyQueue.writeIndex;
    if ((r == 0 && w == BF_REPLY_RING_QUEUE_SIZE - 1)
            || (w + 1 == r))
    {
        pthread_mutex_unlock(&mutexReplyQue);
        return 0;
    }
    
    int len = ReplyQueueLen();
    g_stReplyQueue.replys[g_stReplyQueue.writeIndex] = *reply;
    if (g_stReplyQueue.writeIndex == BF_REPLY_RING_QUEUE_SIZE - 1)
        g_stReplyQueue.writeIndex = 0;
    else
        g_stReplyQueue.writeIndex++;
    notifyMainThread();

    pthread_mutex_unlock(&mutexReplyQue);
    return len + 1;
}

static inline int PopReply(struct BFReply* reply)
{
    if (g_stReplyQueue.readIndex == g_stReplyQueue.writeIndex)
        return 0;

    int len = ReplyQueueLen();
    *reply = g_stReplyQueue.replys[g_stReplyQueue.readIndex];
    if (g_stReplyQueue.readIndex == BF_REPLY_RING_QUEUE_SIZE - 1)
       g_stReplyQueue.readIndex = 0;
    else
        g_stReplyQueue.readIndex++;
    
    return len; 
}

#endif
