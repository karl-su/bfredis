/**********************************************************************************************
*==============================================================================================
*概念:
*
*----------------------------------------------------------------------------------------------
*
*BLOCK      : 即BITMAP, BLOOM FILTER的存储单元，一块占512M内存，2^32个bit
*KEY        : BLOCK作为value在redis中对应的key
*HashThread : 用于计算BLOOM Hash的hash辅助线程
*AllocThread: 用于分配BLOCK内存的alloc辅助线程
*FuncThread : 用于做BLOOMFILTER主体运算的线程池
*
*----------------------------------------------------------------------------------------------
*
*==============================================================================================
*
*
*
*==============================================================================================
*命令:
*
*----------------------------------------------------------------------------------------------
*BFGET
*BFGET key hashcnt str1[ str2 str3 ...]
*查看str1、str2、str3等字符串通过hashcnt次BLOOM hash计算后在BLOCK中是否存在
*返回与str参数个数相等元素个数的数组，数组元素的值为[0, 1], 0代表对应顺序的str没有被设置过
*1代表对应顺序的str被设置过
*
*----------------------------------------------------------------------------------------------
*
*BFSET
*BFGET key hashcnt str1[ str2 str3 ...]
*将str1、str2、str3等字符串通过hashcnt次BLOOM hash计算后，设置到BLOCK中
*返回与str参数个数相等元素个数的数组，数组元素的值为[0, 1], 0代表对应顺序的str没有被设置过
*1代表对应顺序的str被设置过
*
*----------------------------------------------------------------------------------------------
*
*BFBGET key hashcnt strLen strBlock
*strBlock是一个由n个长度weistrLen的字符串组成的字符串块(n === len(strBlock) / strLen)
*查看strBlock中的字符串通过hashcnt次BLOOM hash计算后在BLOCK中是否存在
*返回与参数中字符串个数相等长度的字符串，字符串中每个字节的值为[0, 1], 0代表对应顺序的str没有被设置过
*1代表对应顺序的str被设置过
*
*----------------------------------------------------------------------------------------------
*
*BFBSET key hashcnt strLen strBlock
*strBlock是一个由n个长度为strLen的字符串组成的字符串块(n === len(strBlock) / strLen)
*将strBlock中的字符串通过hashcnt次BLOOM hash计算后，设置到BLOCK中
*返回与参数中字符串个数相等长度的字符串，字符串中每个字节的值为[0, 1], 0代表对应顺序的str没有被设置过
*1代表对应顺序的str被设置过
*
*----------------------------------------------------------------------------------------------
*
*BFCONF
*BFCONF ThrThreshold number
*设置启用hash辅助线程的阈值
*当BFGET、BFSET、BFBGET、BFBSET命令处理的str超过number时，hash辅助线程会辅助主线程计算str的hash值
*成功返回OK
*
*BFCONF MaxFreeBlocks number
*设置单个bfredis中最多存在的用于分配成BLOCK的512M的内存块数
*成功返回OK
*
*BFCONF SyncSetBitMap (0|1)
*设置写bitmap时是否加锁同步,为1时加锁同步
*成功返回OK
*
*BFCONF MaxReqQueLen number
*设置发往线程池的请求队列的最大长度
*成功返回OK
*
*BFCONF SetSlowLog number_slowlog_threshold number_slowlog_size
*设置慢操作日志的记录阈值和最大记录数量
*成功返回OK
*
*BFCONF GetSlowLog len
*获取len条慢操作日志信息
*成功返回慢操作日志信息
*
*BFCONF INFO
*查看bfredis配置信息及指标信息，包括
*   max_free_blocks     : 配置项， 限制单个bfredis中最多存在的用于分配成BLOCK的512M的内存块数
*   cur_free_blocks     : 指标项， 当前bfredis中存在的512M空闲内存块数
*   block_alloc_hits    : 指标项， 使用空闲内存块来生成BLOCK的次数
*   block_alloc_nohits  : 指标项， 临时通过malloc及memset分配内存来生成BLOCK的次数
*   thread_alloc_cnt    : 指标项， ALLOC Thread分配512M内存块的次数
*
*   thread_threshold    : 配置项， 启动hash辅助线程的阈值，当get或set操作的str超过thread_threshold值时，会通知hash辅助线程帮助计算
*
*	func_thread_num		: 配置项,  线程池线程数量
*
*	sync_set_bitmap		: 配置项,  多线程是否互斥写bitmap
*
*   gethit              : 指标项， 做BFGET或BFBGET操作的主线程采纳hash辅助线程计算的hash值的次数
*   getnohit            : 指标项， 做BFGET或BFBGET操作的主线程自己计算hash函数的次数
*   sethit              : 指标项， 做BFSET或BFBSET操作的主线程采纳hash辅助线程计算的hash值的次数
*   setnohit            : 指标项， 做BFSET或BFBSET操作的主线程自己计算hash函数的次数
*   hashthread_used     : 指标项， hash辅助线程被启用的次数
*   thread_calc_hash    : 指标项， hash辅助线程计算hash的次数
*   thread_set_hash     : 指标项， hash辅助线程设置hash值的次数，hash辅助函数会根据主线程的需要计算hash值，计算好后会再次判断这个hash值主线程是否需要
*
*	slowlog_threshold	: 配置项,  记录slowlog的耗时阈值(us)
*	slowlog_size		: 配置项,  记录slowlog的最大条数
*	slowlog_len			: 指标项,  当前已经记录slowlog的条数
*
*	max_request_queue_len:配置项,  request queue最多存放的请求数量
*	request_queue_len	: 指标项,  请求队列的长度
*	reply_queue_len     : 指标项,  响应队列的长度
*
*	BFBGET				: 指标项， 依次为bfbget函数的调用次数、总耗用时长(us)、最大耗用时长(us)、平均耗用时长(us)、主线程处理次数
*	BFBSET				: 指标项， 依次为bfbset函数的调用次数、总耗用时长(us)、最大耗用时长(us)、平均耗用时长(us)、主线程处理次数
*
*
*
*BFCONF ClearStat
*将getbit、getnohit、sethit、setnohit、hashthread_used、thread_calc_hash、thread_set_hash、BFBGET、BFBSET、slowlog指标项清零
*返回OK
*
*----------------------------------------------------------------------------------------------
*
*BFALLOC
*BFALLOC key
*通知bfredis提前分配一个512M的内存块，命令会立即返回，真正的配置内存及初始化内存的操作在AllocThread中完成
*key仅用于bfredis路由命令到某个节点
*成功返回当前bfredis节点中512M空闲内存块数量
*失败返回-1或-2
*
*----------------------------------------------------------------------------------------------
*
*==============================================================================================
*
**************************************************************************************************/
