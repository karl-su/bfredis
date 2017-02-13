/* BloomFilter operations.
 *
 * My English is so poor! 
 * So I can't descript it by Engligsh,
 * And I'm also disinclined to descript it by Chinese.
 * So if you couldn't understand what's this,
 * You can beg God to help you.   
 */

#include "server.h"
#include <sys/time.h>
#include <fcntl.h>
#include <assert.h>
#include "bf_ringque.h"

/* 命令定义 */
#define BF_CMD_BFBGET 1
#define BF_CMD_BFBSET 2

static unsigned char g_bfHashThreadStop = 0;
static pthread_t g_bfHashThread;
/* 主线程与BF HASH计算线程通信的管道描述符 */
static int g_bfHashReadFd;
static int g_bfHashWriteFd;

static unsigned char g_bfAllocThreadStop = 0;
static pthread_t g_bfAllocThread;
/* 主线程与BF BLOCK ALLOC线程通信的管道描述符 */
static int g_bfAllocReadFd;
static int g_bfAllocWriteFd;

#define BF_MAX_FUNC_THREAD_NUM 1024
static unsigned char g_bfThreadFuncStop = 0;
static pthread_t g_bfFuncThreads[BF_MAX_FUNC_THREAD_NUM];
static int g_bfFuncReadFd;
static int g_bfFuncWriteFd;

/* Take From bitopt.c 
 * To Find the key when bfset, if the key not exist then create */
extern robj *lookupStringForBitCommand(client *c, size_t maxbit);
extern void rdbRemoveTempFile(pid_t childpid);
void addReplyBulkCBuffer(client *c, const void *p, size_t len);

struct BFConf {
    uint32_t    maxFreeBlocks;     //最大的允许存在的空闲BLOCK
    uint32_t    hitFreeBlocks;
    uint32_t    nohitFreeBlocks;
    uint32_t    allocByThread;
    
    uint32_t    funcThreadNum;
    uint8_t     syncSetBitMap;
    
    uint32_t    maxReqQueLen;

    uint32_t    hzCron;

    uint64_t    slowThreshold;
    uint64_t    slowLogSize;

    uint64_t    bfbgetCalls;
    uint64_t    bfbgetCost;
    uint64_t    bfbgetMaxCost;
    uint64_t    bfbgetNoInThread;

    uint64_t    bfbsetCalls;
    uint64_t    bfbsetCost;
    uint64_t    bfbsetMaxCost;
    uint64_t    bfbsetNoInThread;
};
static struct BFConf g_bfConf = {4, 0, 0, 0, 8, 1, BF_REQUEST_RING_QUEUE_SIZE - 1, 499, 10000, 100, 0, 0, 0, 0, 0, 0, 0, 0};

/* SLOW LOG 信息 */
struct BFSlowLog {
    uint64_t id;

    int method;
    int hashcnt;
    int slen;

    uint64_t callTime;
    uint64_t enReqQueTime;
    uint64_t deReqQueTime;
    uint64_t enRspQueTime;
    uint64_t deRspQueTime;
    uint64_t doneTime;

    uint32_t reqQueLen;
    uint32_t rspQueLen;

    uint32_t existCnt;
    uint32_t noExistCnt;
};
static list* g_bfSlowLogs;

/* BLOOM FILTER HASH 函数计算数据结构 */
const unsigned int bfseed[] =
{
    0xAAAAAAAA, 0x55555555, 0x33333333, 0xCCCCCCCC,
    0x66666666, 0x99999999, 0xB5B5B5B5, 0x4B4B4B4B,
    0xAA55AA55, 0x55335533, 0x33CC33CC, 0xCC66CC66,
    0x66996699, 0x99B599B5, 0xB54BB54B, 0x4BAA4BAA,
    0xAA33AA33, 0x55CC55CC, 0x33663366, 0xCC99CC99,
    0x66B566B5, 0x994B994B, 0xB5AAB5AA, 0xAAAAAA33,
    0x555555CC, 0x33333366, 0xCCCCCC99, 0x666666B5,
    0x9999994B, 0xB5B5B5AA, 0xFFFFFFFF, 0xFFFF0000
};

//最多允许10240个自定义KEY
#define BF_MAX_CUSTOM_KEYS  10240
//用于发送分布式计算请求的结构体
struct STHashRequest {
    unsigned int    version;
    unsigned int    argc;
    unsigned int    hashcnt;
    
    /* bfget bfset使用 */
    robj**          objStrs;
    /*******************/

    /* bfbget bfbset使用 */
    unsigned int    keyLen;
    robj*           objOne;
    /*********************/
} __attribute__ ((packed));

//用于分布式计算存储hash结果的结构体
struct STHashResult {
    volatile unsigned int   version;                                                            //当前版本, 从1开始递增
    volatile unsigned int   result[BF_MAX_CUSTOM_KEYS][(sizeof(bfseed) / sizeof(bfseed[0])) * 2]; //[rversion][result]
};
static volatile struct STHashResult g_stHashResult = {0, {{0}}};

static inline unsigned int bfhash(const unsigned char* str, size_t len, unsigned int hash)
{
    for (size_t i = 0; i < len; i++)
    {
        hash ^= 
            (((i & 1) == 0) ? 
                ((hash <<  7) ^ (str[i] * (hash >> 3))) 
                : (~(((hash << 11) + str[i]) ^(hash >> 5))));
    }

    return hash;
}

/* BF HASH Calculate Thread Function */
void* BFHashCalcInThread(void* arg)
{
    arg = NULL;
    unsigned int    preArgc = 0;
    robj**          preObjStrs = NULL;
    robj*           preObjOne = NULL;
    struct STHashRequest req;
    
    while (!g_bfHashThreadStop)
    {
        ssize_t recvd = read(g_bfHashReadFd, &req, sizeof(req));
        if (recvd <= 0)
        {
            if (errno == EAGAIN)
            {
                continue;
            }
            else
            {
                fprintf(stderr, "bfhash thread exit! fd:%d, ret:%ld, error:%s\n",
                        g_bfHashReadFd, recvd, strerror(errno));
                serverLog(LL_WARNING, "bfhash thread exit! fd:%d, ret:%ld, error:%s\n",
                        g_bfHashReadFd, recvd, strerror(errno));
                rdbRemoveTempFile(getpid());
                exit(1);
            }
        }

        assert(recvd == sizeof(req));
        
        server.bf_threadused++;

        if (preObjOne)
        {
            decrRefCount(preObjOne);
        }
        else if (preObjStrs)
        {
            for (int i = 0; i < (int)preArgc; i++)
                if (preObjStrs[i] != NULL)
                    decrRefCount(preObjStrs[i]);
            if (preObjStrs)
                zfree(preObjStrs);
        }

        preArgc = req.argc;
        preObjStrs = req.objStrs;
        preObjOne = req.objOne;
                
        if (req.version < g_stHashResult.version)
            continue;
        if (req.hashcnt > (int)(sizeof(bfseed) / sizeof(bfseed[0]))
                || req.hashcnt <= 0)
            continue;
 
        for (int i = 0; i < (int)req.argc && i < (int)BF_MAX_CUSTOM_KEYS; i++)
        {
            if (req.version < g_stHashResult.version)
            {
                break;
            }

            if (g_stHashResult.result[i][0] >= req.version)
            {
                continue;
            }
            
            for (int j = 0; j < (int)req.hashcnt; j++)
            {
                if (g_stHashResult.result[i][j << 1] >= req.version 
                        || g_stHashResult.result[i][(req.hashcnt - 1) << 1] >= req.version
                        || req.version < g_stHashResult.version)
                {
                    break;
                }
                
                server.bf_hashcalcbythread++;
                unsigned int val = bfhash(req.objOne ? 
                        (const uint8_t*)req.objOne->ptr + (i * req.keyLen) : req.objStrs[i]->ptr, 
                        req.objOne ? req.keyLen : sdslen(req.objStrs[i]->ptr), bfseed[j]);
                if (g_stHashResult.result[i][j << 1] < req.version)
                {
                    g_stHashResult.result[i][(j << 1) + 1] = val;
                    g_stHashResult.result[i][(j << 1)] = req.version;

                    server.bf_threadsethash++;
                }
                else
                {
                    break;
                }
            }
        }
    }

    return NULL;   
}

static void BFRecordSlowLog(struct BFReply* reply)
{
    static uint64_t id = 0;

    struct BFSlowLog* slowlog = zmalloc(sizeof(struct BFSlowLog));
    slowlog->id = id++;
    slowlog->method = reply->method;
    slowlog->hashcnt = reply->hashcnt;
    slowlog->slen = reply->len;
    
    slowlog->callTime = reply->callTime;
    slowlog->enReqQueTime = reply->enReqQueTime;
    slowlog->deReqQueTime = reply->deReqQueTime;
    slowlog->enRspQueTime = reply->enRspQueTime;
    slowlog->deRspQueTime = reply->deRspQueTime;
    slowlog->doneTime = reply->doneTime;

    slowlog->reqQueLen = reply->reqQueLen;
    slowlog->rspQueLen = reply->rspQueLen;

    slowlog->existCnt = reply->existCnt;
    slowlog->noExistCnt = reply->noExistCnt;

    listAddNodeHead(g_bfSlowLogs, slowlog);

    while (listLength(g_bfSlowLogs) > g_bfConf.slowLogSize)
        listDelNode(g_bfSlowLogs, listLast(g_bfSlowLogs));
}

/* BFCONF 
 *          thr_threshold count
 *          clearstat*/
void bfconfCommand(client *c) 
{
    if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "thrthreshold")) 
    {
        long long threshold;

        if (getLongLongFromObject(c->argv[2], &threshold) != C_OK || threshold < 0) {
            addReplyErrorFormat(c,"Invalid THREAD THRESHOLD specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }

        server.bf_thrthreshold = (unsigned long long)threshold;
        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "clearstat"))
    {
        server.bf_gethit = 0;
        server.bf_getnohit = 0;
        server.bf_sethit = 0;
        server.bf_setnohit = 0;
        server.bf_threadused = 0,
        server.bf_hashcalcbythread = 0;
        server.bf_threadsethash = 0;
        
        g_bfConf.bfbgetCalls = 0;
        g_bfConf.bfbgetCost = 0;
        g_bfConf.bfbgetMaxCost = 0;
        g_bfConf.bfbgetNoInThread = 0;
        g_bfConf.bfbsetCalls = 0;
        g_bfConf.bfbsetCost = 0;
        g_bfConf.bfbsetMaxCost = 0;
        g_bfConf.bfbsetNoInThread = 0;

        while (listLength(g_bfSlowLogs) > 0)
            listDelNode(g_bfSlowLogs, listLast(g_bfSlowLogs));

        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "maxfreeblocks"))
    {
        long long maxFreeBlocks;

        if (getLongLongFromObject(c->argv[2], &maxFreeBlocks) != C_OK 
                || maxFreeBlocks < 0
                || maxFreeBlocks >= BF_BLOCK_RING_QUEUE_SIZE) {
            addReplyErrorFormat(c,"Invalid Max Free Blocks ( >=0 and <16) specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }

        g_bfConf.maxFreeBlocks = (unsigned int)maxFreeBlocks;
        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "syncsetbitmap"))
    {
        long long syncSetBitMap;

        if (getLongLongFromObject(c->argv[2], &syncSetBitMap) != C_OK) {
            addReplyErrorFormat(c,"Invalid Sync Set BitMap CONFIG specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }

        g_bfConf.syncSetBitMap = (uint8_t)syncSetBitMap;
        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "maxreqquelen"))
    {
        long long maxReqQueLen;

        if (getLongLongFromObject(c->argv[2], &maxReqQueLen) != C_OK 
                || maxReqQueLen < 0
                || maxReqQueLen >= BF_REQUEST_RING_QUEUE_SIZE) {
            addReplyErrorFormat(c,"Invalid Max Request Queue Len ( >=0 and <512) specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }

        g_bfConf.maxReqQueLen = (uint32_t)maxReqQueLen;
        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "hzcron"))
    {
        long long hzCron;

        if (getLongLongFromObject(c->argv[2], &hzCron) != C_OK 
                || hzCron <= 0
                || hzCron > 1000) {
            addReplyErrorFormat(c,"Invalid CRON HZ ( >0 and <=1000) specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }

        g_bfConf.hzCron= (uint32_t)(hzCron - 1);
        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 4 && !strcasecmp(c->argv[1]->ptr, "setslowlog"))
    {
        long long slowThreshold, slowLogSize;

        if (getLongLongFromObject(c->argv[2], &slowThreshold) != C_OK) {
            addReplyErrorFormat(c,"Invalid slowThreshold specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }

        if (getLongLongFromObject(c->argv[3], &slowLogSize) != C_OK) {
            addReplyErrorFormat(c,"Invalid slowLogSize specified: %s",
                                (char*)c->argv[3]->ptr);
            return ;
        }

        g_bfConf.slowThreshold = slowThreshold;
        g_bfConf.slowLogSize = slowLogSize;

        addReply(c,shared.ok);
        return ;
    }
    else if (c->argc == 3 && !strcasecmp(c->argv[1]->ptr, "getslowlog"))
    {
        long long cnt;
        if (getLongLongFromObject(c->argv[2], &cnt) != C_OK
                || cnt == 0) {
            addReplyErrorFormat(c,"Invalid slowlog count specified: %s",
                                (char*)c->argv[2]->ptr);
            return ;
        }
        
        listIter li;
        listNode* ln;
        listRewind(g_bfSlowLogs, &li);
        long number = 0;
        void* pNumber = addDeferredMultiBulkLength(c);
        while (cnt-- && (ln = listNext(&li)))
        {
            struct BFSlowLog* slowlog = ln->value;
            char timebuf[128] = {0};
            time_t t = slowlog->callTime / 1000000;
            strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", localtime(&t));

            sds loginfo = sdsempty();
            loginfo = sdscatprintf(loginfo, "id: %lu, time: %s.%06lu, method: %s, "
                   "hashcnt: %u, strs: %u, strlen: %u, "
                   "exists: %u, noexist: %u, "
                   "cost: %lu, costInMainPre: %lu, costInReqQue: %lu, "
                   "costInThread: %lu, costInRspQue: %lu, costInMainSuff: %lu, "
                   "reqQueLen: %u, rspQueLen: %u",
                   slowlog->id,
                   timebuf, slowlog->callTime % 1000000,
                   (slowlog->method == BF_CMD_BFBSET) ? "BFBSET" : "BFBGET",
                   slowlog->hashcnt, slowlog->existCnt + slowlog->noExistCnt, slowlog->slen,
                   slowlog->existCnt, slowlog->noExistCnt,
                   slowlog->doneTime - slowlog->callTime, slowlog->enReqQueTime - slowlog->callTime,
                   slowlog->deReqQueTime - slowlog->enReqQueTime,
                   slowlog->enRspQueTime - slowlog->deReqQueTime, 
                   slowlog->deRspQueTime - slowlog->enRspQueTime,
                   slowlog->doneTime - slowlog->deRspQueTime,
                   slowlog->reqQueLen, slowlog->rspQueLen);
            addReplyBulkSds(c, loginfo);
            number++;
        }
        setDeferredMultiBulkLength(c, pNumber, number);

        return ;
    }
    else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "info"))
    {
        void* infoReplyLen = addDeferredMultiBulkLength(c);
        long number = 0;

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "max_free_blocks: %u, cur_free_blocks: %u, "
                    "block_alloc_hits: %u, block_alloc_nohits: %u, thread_alloc_cnt: %u", 
                    g_bfConf.maxFreeBlocks, BlockQueueLen(),
                    g_bfConf.hitFreeBlocks, g_bfConf.nohitFreeBlocks, g_bfConf.allocByThread);
            addReplyBulkSds(c, info);
            number++;
        }

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "thread_threshold: %u", (uint32_t)server.bf_thrthreshold);
            addReplyBulkSds(c, info);
            number++;
        }
        
        {
            sds info = sdsempty();
            info = sdscatprintf(info, "func_thread_num: %u", g_bfConf.funcThreadNum);
            addReplyBulkSds(c, info);
            number++;
        }
        
        {
            sds info = sdsempty();
            info = sdscatprintf(info, "sync_set_bitmap: %u", g_bfConf.syncSetBitMap);
            addReplyBulkSds(c, info);
            number++;
        }

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "hz_cron: %u", 
                    1000 / ((1000 / (g_bfConf.hzCron + 1)) ? (1000 / (g_bfConf.hzCron + 1)) : 1001));
            addReplyBulkSds(c, info);
            number++;
        }
        
        {
            sds info = sdsempty();
            info = sdscatprintf(info, "gethit: %llu, getnohit: %llu, "
                    "sethit: %llu, setnohit: %llu, hashthread_used: %llu, thread_calc_hash: %llu, thread_set_hash: %llu", 
                    server.bf_gethit, server.bf_getnohit, server.bf_sethit, server.bf_setnohit,
                    server.bf_threadused, server.bf_hashcalcbythread, server.bf_threadsethash);
            addReplyBulkSds(c, info);
            number++;
        }

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "slowlog_threshold: %u, slowlog_size: %u, slowlog_len: %u", 
                    (uint32_t)g_bfConf.slowThreshold, (uint32_t)g_bfConf.slowLogSize, (uint32_t)listLength(g_bfSlowLogs));
            addReplyBulkSds(c, info);
            number++;
        }

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "max_request_queue_len: %u, request_queue_len: %u, reply_queue_len: %u", 
                    g_bfConf.maxReqQueLen, (uint32_t)ReqQueueLen(), (uint32_t)ReplyQueueLen());
            addReplyBulkSds(c, info);
            number++;
        }

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "BFBGET calls: %lu, cost: %lu, maxcost: %lu, avgcost: %.2lf, deal_in_main: %lu", 
                    g_bfConf.bfbgetCalls, g_bfConf.bfbgetCost, g_bfConf.bfbgetMaxCost, 
                    g_bfConf.bfbgetCalls ? (double)g_bfConf.bfbgetCost / g_bfConf.bfbgetCalls : 0,
                    g_bfConf.bfbgetNoInThread);
            addReplyBulkSds(c, info);
            number++;
        }

        {
            sds info = sdsempty();
            info = sdscatprintf(info, "BFBSET calls: %lu, cost: %lu, maxcost: %lu, avgcost: %.2lf, deal_in_main: %lu", 
                    g_bfConf.bfbsetCalls, g_bfConf.bfbsetCost, g_bfConf.bfbsetMaxCost, 
                    g_bfConf.bfbsetCalls ? (double)g_bfConf.bfbsetCost / g_bfConf.bfbsetCalls : 0,
                    g_bfConf.bfbsetNoInThread);
            addReplyBulkSds(c, info);
            number++;
        }
           
        setDeferredMultiBulkLength(c, infoReplyLen, number);
        return ;
    }

    char* err = "Only Suppoer: "
                "1. BFCONF thrthreshold (number) ;   "
                "2. BFCONF clearstat ;   "
                "3. BFCONF maxfreeblocks (number) ;   "
                "4. BFCONF syncsetbitmap (0|1) ;   "
                "5. BFCONF maxreqquelen (number) ;   "
                "6. BFCONF hzcron (number) ;   "
                "7. BFCONF setslowlog (number_slowlog_threshold) (number_slowlog_size) ;   "
                "8. BFCONF getslowlog (number_get_len) ;   "
                "9. BFCONF info";
    addReplyErrorFormat(c, err);
    return ;
}

/* BFGET key hashcnt customKey1 [customKey2 ...]*/
void bfgetCommand(client *c) {
    robj *o;
    char llbuf[32];
    size_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;

    int haskey = 1;
    int hasAssistant = 0;
    int iarg = 0;
    int seedNum = (int)sizeof(bfseed) / sizeof(bfseed[0]);
    long hashNum = 0;

    if ((o = lookupKeyRead(c->db, c->argv[1])) == NULL ||
        checkType(c,o,OBJ_STRING)) 
    {
        haskey = 0;
    }

    if (getLongFromObjectOrReply(c, c->argv[2], &hashNum, NULL) != C_OK
        || hashNum <= 0 || hashNum > seedNum)
    {
        addReplyError(c, "hash number is invalid");
        return ;   
    }
    
    //发送hash计算请求, 当customKey的个数大于27个时，才启用辅助线程 ()
    if (haskey && (c->argc - 3) > (int)server.bf_thrthreshold)
    {
        struct STHashRequest req;
        req.version = ++g_stHashResult.version;
        req.argc = c->argc - 3;
        req.keyLen = 0;
        req.objOne = NULL;
        req.hashcnt = hashNum;
        req.objStrs = zmalloc(sizeof(robj*) * req.argc);
        for (int i = 3; i < c->argc; i++)
            req.objStrs[i - 3] = c->argv[i];

        if (write(g_bfHashWriteFd, &req, sizeof(req)) > 0)
        {
            hasAssistant = 1;
            for (int i = 3; i < c->argc; i++)
                incrRefCount(c->argv[i]);
        }
        else
        {
            zfree(req.objStrs);
        }
    }

    void* replyLen = addDeferredMultiBulkLength(c);
    int replyElements = 0;
    for (iarg = 3; iarg < c->argc; iarg++)
    {
        if (haskey == 0)
        {
            addReply(c, shared.czero);
            replyElements++;
            continue;
        }

        int customKeySeted = 1;
        for (int i = 0; i < hashNum; i++)
        {
            if (hasAssistant && iarg - 3 < BF_MAX_CUSTOM_KEYS 
                    && g_stHashResult.result[iarg - 3][i << 1] == g_stHashResult.version)
            {
                bitoffset = g_stHashResult.result[iarg - 3][(i << 1) + 1];
                server.bf_gethit++;
            }
            else
            {
                if (hasAssistant && iarg - 3 < BF_MAX_CUSTOM_KEYS)
                    g_stHashResult.result[iarg - 3][i << 1] = g_stHashResult.version;

                bitoffset = bfhash(c->argv[iarg]->ptr, sdslen(c->argv[iarg]->ptr), bfseed[i]);
                server.bf_getnohit++;
            }
            
            //fprintf(stderr, "custom key[%d]:%s, bitoffset:%lu\n", i, c->argv[iarg]->ptr, bitoffset);
            //bitoffset = bitoffset % (4 * 1024 * 1024 * 1024);
            byte = bitoffset >> 3;
            bit = 7 - (bitoffset & 0x7);
            if (sdsEncodedObject(o)) 
            {
                if (byte < sdslen(o->ptr))
                    bitval = ((uint8_t*)o->ptr)[byte] & (1 << bit);
            } 
            else 
            {
                if (byte < (size_t)ll2string(llbuf,sizeof(llbuf),(long)o->ptr))
                    bitval = llbuf[byte] & (1 << bit);
            }
            
            if (!bitval)
            {
                if (hasAssistant && iarg - 3 < BF_MAX_CUSTOM_KEYS)
                    g_stHashResult.result[iarg - 3][(hashNum - 1) << 1] = g_stHashResult.version;

                customKeySeted = 0;
                break;
            }
        }
        addReply(c, customKeySeted ? shared.cone : shared.czero);
        replyElements++;
    }
    setDeferredMultiBulkLength(c, replyLen, replyElements);
}

/* BFSET key hashcnt customKey1 [customKey2 ...]*/
void bfsetCommand(client *c) {
    robj *o;
    size_t bitoffset;
    size_t byte, bit;
    int byteval, bitval;
    long on = 1;

    int hasAssistant = 0;
    int iarg = 0;
    int seedNum = (int)sizeof(bfseed) / sizeof(bfseed[0]);
    long hashNum = 0;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) 
    {
        sds b = PopBlock();
        if (b)
        {
            o = createObject(OBJ_STRING, b);
            g_bfConf.hitFreeBlocks++;
        }
        else
        {
            o = createObject(OBJ_STRING, sdsnewlen(NULL, 512 * 1024 * 1024));
            g_bfConf.nohitFreeBlocks++;
        }
        dbAdd(c->db,c->argv[1],o);
    } 
    else 
    {
        if (checkType(c, o, OBJ_STRING)) 
        {
            addReplyError(c, "value type is invalid");
            return ;
        }
        o = dbUnshareStringValue(c->db, c->argv[1], o);
        o->ptr = sdsgrowzero(o->ptr, 512 * 1024 * 1024);
    }

    if (getLongFromObjectOrReply(c, c->argv[2], &hashNum, NULL) != C_OK
        || hashNum <= 0 || hashNum > seedNum)
    {
        addReplyError(c, "hash number is invalid");
        return ;   
    }

    //发送hash计算请求, 当customKey的个数大于27个时，才启用辅助线程
    if (c->argc - 3 > (int)server.bf_thrthreshold)
    {
        struct STHashRequest req;
        req.version = ++g_stHashResult.version;
        req.argc = c->argc - 3;
        req.keyLen = 0;
        req.objOne = NULL;
        req.hashcnt = hashNum;
        req.objStrs = zmalloc(sizeof(robj*) * req.argc);
        for (int i = 3; i < c->argc; i++)
            req.objStrs[i - 3] = c->argv[i];
        if (write(g_bfHashWriteFd, &req, sizeof(req)) > 0)
        {
            hasAssistant = 1;
            for (int i = 3; i < c->argc; i++)
                incrRefCount(c->argv[i]);
        }
        else
        {
            zfree(req.objStrs);
        }
    }
    
    void* replyLen = addDeferredMultiBulkLength(c);
    int replyElements = 0;
    for (iarg = 3; iarg < c->argc; iarg++)
    {
        int customKeySeted = 1;
        for (int i = 0; i < hashNum; i++)
        {
            if (hasAssistant && iarg - 3 < BF_MAX_CUSTOM_KEYS
                    && g_stHashResult.result[iarg - 3][i << 1] == g_stHashResult.version)
            {
                server.bf_sethit++;
                bitoffset = g_stHashResult.result[iarg - 3][(i << 1) + 1];
            }
            else
            {
                if (hasAssistant && iarg - 3 < BF_MAX_CUSTOM_KEYS)
                    g_stHashResult.result[iarg - 3][i << 1] = g_stHashResult.version;
                bitoffset = bfhash(c->argv[iarg]->ptr, sdslen(c->argv[iarg]->ptr), bfseed[i]);
                server.bf_setnohit++;
            }
            //fprintf(stderr, "custom key[%d]:%s, bitoffset:%lu\n", i, c->argv[iarg]->ptr, bitoffset);
            //bitoffset = bitoffset % (4 * 1024 * 1024 * 1024);

            /* Get current values */
            byte = bitoffset >> 3;
            byteval = ((uint8_t*)o->ptr)[byte];
            bit = 7 - (bitoffset & 0x7);
            bitval = byteval & (1 << bit);
            if (!bitval)
            {
                customKeySeted = 0;
            }

            /* Update byte with new bit value and return original value */
            byteval &= ~(1 << bit);
            byteval |= ((on & 0x1) << bit);
            ((uint8_t*)o->ptr)[byte] = byteval;
        }
        addReply(c, customKeySeted ? shared.cone : shared.czero);
        replyElements++;
    }
    setDeferredMultiBulkLength(c, replyLen, replyElements);
    
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"bfset",c->argv[1],c->db->id);
    server.dirty++;
}

int dealReplyList(client* c, uint64_t seq, sds result)
{
    listNode* ln;
    int cnt = 0;

    //清除非法的计算结果
    while ((ln = listFirst(c->replys)))
    {
        replyList* r = listNodeValue(ln);
        if (r->seq > c->expireSeq)
        {
            break;
        }
        else if (r->seq < c->expireSeq)
        {
            sdsfree(r->reply);
            listDelNode(c->replys, ln);
            zfree(r);
        }
        else if (r->seq == c->expireSeq)
        {
            cnt++;
            addReplyBulkSds(c, r->reply);
            c->expireSeq++;

            listDelNode(c->replys, ln);
            zfree(r);
        }
    }

    if (seq < c->expireSeq)
    {
        sdsfree(result);
        return cnt;       
    }
    else if (seq > c->expireSeq)
    {
        ln = listFirst(c->replys);
        while (ln)
        {
            replyList* r = listNodeValue(ln);
            if (r->seq > seq)
            {
                break;
            }
            else if (r->seq == seq)
            {
                sdsfree(r->reply);
                r->reply = result;
                return cnt;
            }
            ln = listNextNode(ln);
        }

        replyList* r = zmalloc(sizeof(replyList));
        if (r == NULL) return cnt;
        r->reply = result;
        r->seq = seq;
        listNode* node = zmalloc(sizeof(listNode));
        if (node == NULL) 
        {  
            sdsfree(result);
            zfree(r);
            return cnt;
        }
        node->value = r;
        if (listLength(c->replys) == 0)
        {
            c->replys->head = c->replys->tail = node;
            node->prev = node->next = NULL;
            c->replys->len++;
        }
        else if (ln)
        {
            node->next = ln;
            node->prev = ln->prev;
            if (c->replys->head == ln)
                c->replys->head = node;
            if (node->prev)
                node->prev->next = node;
            ln->prev = node;
            c->replys->len++;
        }
        else
        {
            node->prev = c->replys->tail;
            node->next = NULL;
            c->replys->tail->next = node;
            c->replys->tail = node;
            c->replys->len++;
        }

        return cnt;
    }

    cnt++;
    addReplyBulkSds(c, result);
    c->expireSeq++;
    while ((ln = listFirst(c->replys)))
    {
        replyList* r = listNodeValue(ln);
        if (r->seq > c->expireSeq)
        {
            break;
        }
        else if (r->seq < c->expireSeq)
        {
            sdsfree(r->reply);
            listDelNode(c->replys, ln);
            zfree(r);
        }
        else if (r->seq == c->expireSeq)
        {
            cnt++;
            addReplyBulkSds(c, r->reply);
            c->expireSeq++;

            listDelNode(c->replys, ln);
            zfree(r);
        }
    }

    return cnt;
}

sds bfbget(robj* strs, int len, int hashcnt, robj* bitmap, int inMain)
{
    UNUSED(inMain);

    char llbuf[32];
    size_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;
 
    int strcnt = sdslen(strs->ptr) / len;
    sds result = sdsnewlen(NULL, strcnt);  
    
    if (!bitmap)
        return result;

    for (int i = 0; i < strcnt; i++)
    {
        uint8_t customKeySeted = 1;
        for (int j = 0; j < hashcnt; j++)
        {
            bitoffset = bfhash((const uint8_t*)strs->ptr + (i * len), len, bfseed[j]);
            byte = bitoffset >> 3;
            bit = 7 - (bitoffset & 0x7);
            if (sdsEncodedObject(bitmap)) 
            {
                if (byte < sdslen(bitmap->ptr))
                    bitval = ((uint8_t*)bitmap->ptr)[byte] & (1 << bit);
            } 
            else 
            {
                if (byte < (size_t)ll2string(llbuf,sizeof(llbuf),(long)bitmap->ptr))
                    bitval = llbuf[byte] & (1 << bit);
            }
            
            if (!bitval)
            {
                customKeySeted = 0;
                break;
            }
        }

        ((uint8_t*)result)[i] = customKeySeted; 
    }

    return result;
}

sds bfbset(robj* strs, int len, int hashcnt, robj* bitmap, int inMain)
{
    UNUSED(inMain);

    size_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;
    int byteval = 0;

    int strcnt = sdslen(strs->ptr) / len;
    sds result = sdsnewlen(NULL, strcnt);  
    
    for (int i = 0; i < strcnt; i++)
    {
        uint8_t customKeySeted = 1;
        for (int j = 0; j < hashcnt; j++)
        {
            bitoffset = bfhash((const uint8_t*)strs->ptr + (i * len), len, bfseed[j]);
            /* Get current values */
            byte = bitoffset >> 3;
            byteval = ((uint8_t*)bitmap->ptr)[byte];
            bit = 7 - (bitoffset & 0x7);
            bitval = byteval & (1 << bit);
            if (!bitval)
            {
                customKeySeted = 0;
                
                if (g_bfConf.syncSetBitMap)
                    __sync_or_and_fetch(((uint8_t*)bitmap->ptr + byte), (0x1 << bit));
                else
                    ((uint8_t*)bitmap->ptr)[byte] |= (0x1 << bit);
            }
        }

        ((uint8_t*)result)[i] = customKeySeted; 
    }

    return result;
}

void notifyMainThread()
{
    int ret = 0;

retry:
    ret = write(g_bfFuncWriteFd, "x", 1);
    if (ret <= 0)
    {
        if (errno == EAGAIN)
        {
            goto retry;      
        }
        else
        {
            serverLog(LL_WARNING,
                "notifyMainThread failed! write pipe error! ret:%d, error:%s", ret, strerror(errno));
            rdbRemoveTempFile(getpid());
            exit(1);
        }
    }
}

int ScanReplyQueue(int inWhich)
{
    UNUSED(inWhich);
    
    int deal = 0;
    struct BFReply reply;
    while (PopReply(&reply) > 0)
    {
        deal++;
        reply.deRspQueTime = ustime();
        if (reply.bitmap) decrRefCount(reply.bitmap);
        decrRefCount(reply.strs);

        reply.client->used--;
        if (!reply.client->valid)
        {
            sdsfree(reply.result);

            if (!reply.client->used)
                zfree(reply.client);
            goto deal_next_reply;
        }

        dealReplyList(reply.client, reply.seq, reply.result);

deal_next_reply:
        {
            reply.doneTime = ustime();
            uint64_t cost = reply.doneTime - reply.callTime;
            if (reply.method == BF_CMD_BFBSET)
            {
                g_bfConf.bfbsetCalls++;
                g_bfConf.bfbsetCost += cost;
                if (cost > g_bfConf.bfbsetMaxCost) 
                    g_bfConf.bfbsetMaxCost = cost; 
            }
            else
            {
                g_bfConf.bfbgetCalls++;
                g_bfConf.bfbgetCost += cost;
                if (cost > g_bfConf.bfbgetMaxCost)
                    g_bfConf.bfbgetMaxCost = cost;
            }

            //记录slowlog
            if (cost > g_bfConf.slowThreshold)
            {
                BFRecordSlowLog(&reply);
            }
        }
    }

    return deal;
}

void bfbCommandSuffProc(aeEventLoop* el, int fd, void* private, int mask)
{
    UNUSED(el);
    UNUSED(private);
    UNUSED(mask);

    static char buf[4096] = {0};
    while (true)
    {
        int nread = read(fd, buf, sizeof(buf));
        if (nread <= 0)
        {
            if (errno == EAGAIN)
            {
                break;
            }
            else
            {
                fprintf(stderr, "bfgetCommandSuffProc read failed! fd:%d, ret:%d, error:%s\n",
                        fd, nread, strerror(errno));
                serverLog(LL_WARNING, "bfgetCommandSuffProc read failed! fd:%d, ret:%d, error:%s\n",
                        fd, nread, strerror(errno));
                rdbRemoveTempFile(getpid());
                exit(1);
            }
        }
        else if (nread < (int)sizeof(buf))
        {
            break;
        }
    }
    
    ScanReplyQueue(0);
}

int bfCron(aeEventLoop* el, long long id, void* clientData)
{
    UNUSED(el);
    UNUSED(id);
    UNUSED(clientData);

    ScanReplyQueue(1);

    return 1000 / (g_bfConf.hzCron + 1);
}

void* bfThreadFunc(void* arg)
{
    UNUSED(arg);

    struct BFRequest req;
    struct BFReply reply;
    
    while (!g_bfThreadFuncStop)
    {
        pthread_mutex_lock(&mutexReqQue);
        if (ReqQueueLen() == 0)
        {
            pthread_cond_wait(&condReqQue, &mutexReqQue);      
        }

        int ret = PopRequest(&req);
        pthread_mutex_unlock(&mutexReqQue);

        if (ret > 0)
        {
            reply.client = req.client;
            reply.seq = req.seq;
            reply.callTime = req.callTime;
            reply.enReqQueTime = req.enReqQueTime;
            reply.deReqQueTime = ustime();
            reply.reqQueLen = req.reqQueLen;
            reply.method = req.method;
            reply.bitmap = req.bitmap;
            reply.strs = req.strs;
            reply.len = req.len;
            reply.hashcnt = req.hashcnt;
            reply.existCnt = 0;
            reply.noExistCnt = 0;

            if (reply.method == BF_CMD_BFBSET)
                reply.result = bfbset(req.strs, req.len, req.hashcnt, req.bitmap, 0);
            else
                reply.result = bfbget(req.strs, req.len, req.hashcnt, req.bitmap, 0);

            for (int i = 0; i < (int)sdslen(reply.result); i++)
            {
                if (((uint8_t*)reply.result)[i])
                    reply.existCnt++;
                else
                    reply.noExistCnt++;
            }

            reply.enRspQueTime = ustime();
            reply.rspQueLen = ReplyQueueLen();
            int ret = PushReply(&reply);
            if (ret <= 0)
            {
                usleep(5000);
                ret = PushReply(&reply);
            }

            if (ret < 0)
            {
                serverLog(LL_WARNING, "bfThreadFunc push reply failed! ret:%d, quelen:%d\n",
                        ret, ReplyQueueLen());
                rdbRemoveTempFile(getpid());
                exit(1);
            }
        }
    }

    return NULL;
}

/* BFBGET key hashcnt customKeyLen customKeyBlock */
void bfbgetCommand(client *c) {
    robj *o;

    long hashNum = 0;
    long keyCnt = 0;
    long keyLen = 0;
    int seedNum = (int)sizeof(bfseed) / sizeof(bfseed[0]);
    uint64_t start = ustime();

    o = lookupKeyRead(c->db, c->argv[1]);

    if (getLongFromObjectOrReply(c, c->argv[2], &hashNum, NULL) != C_OK
        || hashNum <= 0 || hashNum > seedNum)
    {
        addReplyError(c, "hash number is invalid");
        return ;   
    }

    if (getLongFromObjectOrReply(c, c->argv[3], &keyLen, NULL) != C_OK)
    {
        addReplyError(c, "key length is invalid");
        return ;   
    } 

    if (!keyLen || sdslen(c->argv[4]->ptr) % keyLen != 0)
    {
        addReplyError(c, "keylen not match keyblock");
        return ; 
    }
    keyCnt = sdslen(c->argv[4]->ptr) / keyLen;

    if (keyCnt > BF_MAX_CUSTOM_KEYS)
    {
        addReplyError(c, "too mange key");
        return ;
    }
    
    struct BFRequest req;
    req.client = c;
    req.seq = c->seq;
    req.callTime = start;
    req.method = BF_CMD_BFBGET;
    req.bitmap = o;
    req.strs = c->argv[4];
    req.len = keyLen;
    req.hashcnt = hashNum;
    req.enReqQueTime = ustime();
    req.reqQueLen = ReqQueueLen();
    if (g_bfConf.funcThreadNum == 0
            || keyCnt < (long)server.bf_thrthreshold
            || ReplyQueueLen() >= (BF_REPLY_RING_QUEUE_SIZE - BF_REQUEST_RING_QUEUE_SIZE) 
            || req.reqQueLen > g_bfConf.maxReqQueLen
            || PushRequest(&req) <= 0)
    {
        g_bfConf.bfbgetNoInThread++;
        dealReplyList(c, c->seq, bfbget(req.strs, keyLen, hashNum, req.bitmap, 1));
    }
    else
    {
        pthread_cond_signal(&condReqQue);
        if (req.bitmap) incrRefCount(req.bitmap);
        incrRefCount(req.strs);
        c->used++;
    }
    c->seq++;
}

/* BFBSET key hashcnt customKeyLen customKeyBlock */
void bfbsetCommand(client *c) {
    robj *o;
    int seedNum = (int)sizeof(bfseed) / sizeof(bfseed[0]);
    long hashNum = 0;
    long keyCnt = 0;
    long keyLen = 0;
    uint64_t start = ustime();

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) 
    {
        sds b = PopBlock();
        if (b)
        {
            o = createObject(OBJ_STRING, b);
            g_bfConf.hitFreeBlocks++;
        }
        else
        {
            o = createObject(OBJ_STRING, sdsnewlen(NULL, 512 * 1024 * 1024));
            g_bfConf.nohitFreeBlocks++;
        }
        dbAdd(c->db,c->argv[1],o);
    } 
    else 
    {
        if (checkType(c, o, OBJ_STRING)) 
        {
            addReplyError(c, "value type is invalid");
            return ;
        }
        //这里的bitmap可以共享
        //o = dbUnshareStringValue(c->db, c->argv[1], o);
        o->ptr = sdsgrowzero(o->ptr, 512 * 1024 * 1024);
    }

    if (getLongFromObjectOrReply(c, c->argv[2], &hashNum, NULL) != C_OK
        || hashNum <= 0 || hashNum > seedNum)
    {
        addReplyError(c, "hash number is invalid");
        return ;   
    }
    
    if (getLongFromObjectOrReply(c, c->argv[3], &keyLen, NULL) != C_OK)
    {
        addReplyError(c, "key length is invalid");
        return ;   
    } 

    if (!keyLen || sdslen(c->argv[4]->ptr) % keyLen != 0)
    {
        addReplyError(c, "keylen not match keyblock");
        return ; 
    }
    keyCnt = sdslen(c->argv[4]->ptr) / keyLen;

    if (keyCnt > BF_MAX_CUSTOM_KEYS)
    {
        addReplyError(c, "too mange key");
        return ;
    }
    
    struct BFRequest req;
    req.client = c;
    req.seq = c->seq;
    req.callTime = start;
    req.method = BF_CMD_BFBSET;
    req.bitmap = o;
    req.strs = c->argv[4];
    req.len = keyLen;
    req.hashcnt = hashNum;
    req.enReqQueTime = ustime();
    req.reqQueLen = ReqQueueLen();
    if (g_bfConf.funcThreadNum == 0
            || keyCnt < (long)server.bf_thrthreshold
            || ReplyQueueLen() >= (BF_REPLY_RING_QUEUE_SIZE - BF_REQUEST_RING_QUEUE_SIZE) 
            || req.reqQueLen > g_bfConf.maxReqQueLen
            || PushRequest(&req) <= 0)
    {
        g_bfConf.bfbsetNoInThread++;
        dealReplyList(c, c->seq, bfbset(req.strs, keyLen, hashNum, req.bitmap, 1));
    }
    else
    {
        pthread_cond_signal(&condReqQue);
        if (req.bitmap) incrRefCount(req.bitmap);
        incrRefCount(req.strs);
        c->used++;
    }
    c->seq++;
    
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"bfbset",c->argv[1],c->db->id);
    server.dirty++;
}

/* BF Block Alloc Thread Function */
void* BFAllocThread(void* arg)
{
    arg = NULL;
    char buf[2] = {0};
    while (!g_bfAllocThreadStop)
    {
        ssize_t recvd = read(g_bfAllocReadFd, &buf, 1);
        if (recvd <= 0)
        {
            if (errno == EAGAIN)
            {
                continue;
            }
            else
            {
                fprintf(stderr, "bfalloc thread exit! fd:%d, ret:%ld, error:%s\n",
                        g_bfAllocReadFd, recvd, strerror(errno));
                serverLog(LL_WARNING, "bfalloc thread exit! fd:%d, ret:%ld, error:%s\n",
                        g_bfAllocReadFd, recvd, strerror(errno));
                rdbRemoveTempFile(getpid());
                exit(1);
            }
        }

        if (BlockQueueLen() < BF_BLOCK_RING_QUEUE_SIZE - 1 
                && ((int)g_bfConf.maxFreeBlocks > BlockQueueLen()))
        {
            g_bfConf.allocByThread++;
            PushOBlock();
        }
    }

    return NULL;   
}

/* BFALLOC */
void bfallocCommand(client *c) {
    
    int queueLen = BlockQueueLen();
    if (queueLen >= BF_BLOCK_RING_QUEUE_SIZE - 1 
            || ((int)g_bfConf.maxFreeBlocks <= queueLen))
    {
        addReplyLongLong(c, -1);
        return ;
    }

    if (write(g_bfAllocWriteFd, "a", 1) <= 0)
    {
        addReplyLongLong(c, -2);
        return ;
    }

    addReplyLongLong(c, queueLen + 1);
    
    server.dirty++;
}

void uninitBloomFilter()
{
    g_bfHashThreadStop = 1;
    g_bfAllocThreadStop = 1;
    g_bfThreadFuncStop = 1;
    close(g_bfHashReadFd);
    close(g_bfHashWriteFd);
    close(g_bfAllocReadFd);
    close(g_bfAllocWriteFd);
    close(g_bfFuncReadFd);
    close(g_bfFuncWriteFd);
    pthread_join(g_bfHashThread, NULL);
    pthread_join(g_bfAllocThread, NULL);

    for (int i = 0; i < (int)g_bfConf.funcThreadNum; i++)
    {
        pthread_cond_signal(&condReqQue);
    }
    for (int i = 0; i < (int)g_bfConf.funcThreadNum; i++)
    {
        pthread_join(g_bfFuncThreads[i], NULL);
    }

    listRelease(g_bfSlowLogs);
}

void initBloomFilter()
{
    g_bfConf.funcThreadNum = server.bf_functhread_num;
    g_bfSlowLogs = listCreate();
    listSetFreeMethod(g_bfSlowLogs, zfree);

    if (!g_bfSlowLogs)
    {
        serverLog(LL_WARNING,
                    "Create list for BFSlowLog failed! error:%s", strerror(errno));
        exit(1);
    }
    
    /* 创建用于计算BF HASH的线程 */
    int bfHashPipeFds[2] = {-1};
    int bfret = pipe(bfHashPipeFds);
    if (bfret != 0)
    {
        serverLog(LL_WARNING,
                    "Create pipe for BFHASH failed! ret:%d, error:%s", bfret, strerror(errno));
        exit(1);
    }
    g_bfHashReadFd = bfHashPipeFds[0];
    g_bfHashWriteFd = bfHashPipeFds[1];
    int bfflags = fcntl(g_bfHashWriteFd, F_GETFL, 0);
    if (bfflags == -1)
    {
        serverLog(LL_WARNING,
                    "Get bfHashWriteFd flags failed! ret:%d, error:%s", bfflags, strerror(errno));
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        exit(1);
    }
    bfret = fcntl(g_bfHashWriteFd, F_SETFL, bfflags | O_NONBLOCK);
    if (bfret == -1)
    {
        serverLog(LL_WARNING,
                    "Set bfHashWriteFd flags failed! ret:%d, error:%s", bfret, strerror(errno));
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        exit(1);
    }

    bfret = pthread_create(&g_bfHashThread, NULL, BFHashCalcInThread, NULL);
    if (bfret != 0)
    {
        serverLog(LL_WARNING,
                    "Create thread for BFHASH failed! ret:%d, error:%s", bfret, strerror(errno));
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        exit(1);
    }

    /* 创建用于BF 分配内存的线程 */
    int bfAllocPipeFds[2] = {-1};
    bfret = pipe(bfAllocPipeFds);
    if (bfret != 0)
    {
        serverLog(LL_WARNING,
                    "Create pipe for BFAlloc failed! ret:%d, error:%s", bfret, strerror(errno));
        g_bfHashThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        pthread_join(g_bfHashThread, NULL);
        exit(1);
    }

    g_bfAllocReadFd = bfAllocPipeFds[0];
    g_bfAllocWriteFd = bfAllocPipeFds[1];
    bfflags = fcntl(g_bfAllocWriteFd, F_GETFL, 0);
    if (bfflags == -1)
    {
        serverLog(LL_WARNING,
                    "Get bfAllocWriteFd flags failed! ret:%d, error:%s", bfflags, strerror(errno));
        g_bfHashThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        close(g_bfAllocReadFd);
        close(g_bfAllocReadFd);
        pthread_join(g_bfHashThread, NULL);
        exit(1);
    }
    bfret = fcntl(g_bfAllocWriteFd, F_SETFL, bfflags | O_NONBLOCK);
    if (bfret == -1)
    {
        serverLog(LL_WARNING,
                    "Set bfAllocWriteFd flags failed! ret:%d, error:%s", bfret, strerror(errno));
        g_bfHashThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        close(g_bfAllocReadFd);
        close(g_bfAllocWriteFd);
        pthread_join(g_bfHashThread, NULL);
        exit(1);
    }

    bfret = pthread_create(&g_bfAllocThread, NULL, BFAllocThread, NULL);
    if (bfret != 0)
    {
        serverLog(LL_WARNING,
                    "Create thread for BFALLOC failed! ret:%d, error:%s", bfret, strerror(errno));
        g_bfHashThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        close(g_bfAllocReadFd);
        close(g_bfAllocWriteFd);
        pthread_join(g_bfHashThread, NULL);
        exit(1);
    }

    /* 创建用于BF 逻辑运算 */
    int bfFuncPipeFds[2] = {-1};
    bfret = pipe(bfFuncPipeFds);
    if (bfret != 0)
    {
        serverLog(LL_WARNING,
                    "Create pipe for BFFunc failed! ret:%d, error:%s", bfret, strerror(errno));
        g_bfHashThreadStop = 1;
        g_bfAllocThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        close(g_bfAllocReadFd);
        close(g_bfAllocWriteFd);
        pthread_join(g_bfHashThread, NULL);
        pthread_join(g_bfAllocThread, NULL);
        exit(1);
    }

    g_bfFuncReadFd = bfFuncPipeFds[0];
    g_bfFuncWriteFd = bfFuncPipeFds[1];
    bfflags = fcntl(g_bfFuncReadFd, F_GETFL, 0);
    if (bfflags == -1)
    {
        serverLog(LL_WARNING,
                    "Get bfFuncReadFd flags failed! ret:%d, error:%s", bfflags, strerror(errno));
        g_bfHashThreadStop = 1;
        g_bfAllocThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        close(g_bfAllocReadFd);
        close(g_bfAllocWriteFd);
        close(g_bfFuncReadFd);
        close(g_bfFuncWriteFd);
        pthread_join(g_bfHashThread, NULL);
        pthread_join(g_bfAllocThread, NULL);
        exit(1);
    }
    bfret = fcntl(g_bfFuncReadFd, F_SETFL, bfflags | O_NONBLOCK);
    if (bfret == -1)
    {
        serverLog(LL_WARNING,
                    "Set bfFuncReadFd flags failed! ret:%d, error:%s", bfret, strerror(errno));
        g_bfHashThreadStop = 1;
        g_bfAllocThreadStop = 1;
        close(g_bfHashReadFd);
        close(g_bfHashWriteFd);
        close(g_bfAllocReadFd);
        close(g_bfAllocWriteFd);
        close(g_bfFuncReadFd);
        close(g_bfFuncWriteFd);
        pthread_join(g_bfHashThread, NULL);
        pthread_join(g_bfAllocThread, NULL);
        exit(1);
    }

    for (int i = 0; i < (int)g_bfConf.funcThreadNum; i++)
    {
        bfret = pthread_create(&g_bfFuncThreads[i], NULL, bfThreadFunc, NULL);
        if (bfret != 0)
        {
            serverLog(LL_WARNING,
                    "Create thread for BFFUNC(%d) failed! ret:%d, error:%s", i, bfret, strerror(errno));
            g_bfHashThreadStop = 1;
            g_bfAllocThreadStop = 1;
            g_bfThreadFuncStop = 1;
            close(g_bfHashReadFd);
            close(g_bfHashWriteFd);
            close(g_bfAllocReadFd);
            close(g_bfAllocWriteFd);
            close(g_bfFuncReadFd);
            close(g_bfFuncWriteFd);
            pthread_join(g_bfHashThread, NULL);
            pthread_join(g_bfAllocThread, NULL);

            for (int j = 0; j < i; j++)
            {
                pthread_cond_signal(&condReqQue);
            }
            for (int j = 0; j < i; j++)
            {
                pthread_join(g_bfFuncThreads[j], NULL);
            }
            exit(1);
        }
    }

    if (aeCreateFileEvent(server.el, g_bfFuncReadFd, AE_READABLE, 
                bfbCommandSuffProc, NULL) == AE_ERR)
    {
        uninitBloomFilter();
        serverLog(LL_WARNING,
                    "CreateFileEvent for FuncReadFd failed! error:%s", strerror(errno));
        exit(1);
    }

    if (aeCreateTimeEvent(server.el, 1, bfCron, NULL, NULL) == AE_ERR)
    {
        uninitBloomFilter();
        serverLog(LL_WARNING,
                    "CreateTimeEvent for bfCron failed! error:%s", strerror(errno));
        exit(1);
    }
}


