package shardkv

import "sync"

// 清理垃圾
func (kv *ShardKV) PushGC() {
	kv.mu.Lock()
	gid2shardIDs := kv.getShardIDsByStatusL(GCing)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		DPrintf("kvserver: %d , group: %d 开始删除垃圾任务, 删除shardIDs %d 从 group: %d", kv.me, kv.gid, shardIDs, gid)
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			gcTaskRequest := ShardOperationArgs{configNum, shardIds}
			for _, server := range servers {
				var gcTaskResponse ShardOperationReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShardsData", &gcTaskRequest, &gcTaskResponse) && gcTaskResponse.Err == OK {
					DPrintf("kvserver: %d , group: %d 得到了gcTaskResponse %v", kv.me, kv.gid, gcTaskResponse)
					kv.Execute(NewPushGCCommand(&gcTaskRequest), &CommandResponse{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}
func (kv *ShardKV) DeleteShardsData(request *ShardOperationArgs, response *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("kvserver: %d , group: %d 收到了垃圾删除询问, request: %v", kv.me, kv.gid, request)
	// 如果发现请求中的配置版本小于本地的版本，那说明该请求已经执行过,直接返回OK即可
	if kv.currentConfig.Num > request.ConfigNum {
		response.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	var commandResponse CommandResponse
	kv.Execute(NewPushGCCommand(request), &commandResponse)
	DPrintf("kvserver: %d , group: %d 垃圾删除回复 commandResponse: %v", kv.me, kv.gid, commandResponse)
	response.Err = commandResponse.Err
}

// 仅可执行与当前配置版本相同地分片删除日志，否则已经删除过，直接返回 OK 即可。
//
func (kv *ShardKV) applyPushGCL(shardsInfo *ShardOperationArgs) *CommandResponse {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("kvserver: %d , group: %d 开始接受删除 %v", kv.me, kv.gid, shardsInfo)
		for _, shardId := range shardsInfo.ShardIDs {
			shard := kv.kvStore[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.kvStore[shardId] = NewShard()
			} else {
				DPrintf("kvserver: %d , group: %d 重复删除了 shardsInfo : %v", kv.me, kv.gid, shardsInfo)
				break
			}
		}
		DPrintf("kvserver: %d , group: %d 完成接受删除 %v", kv.me, kv.gid, shardsInfo)
		return &CommandResponse{OK, ""}
	}
	DPrintf("kvserver: %d , group: %d 重复删除了 shardsInfo : %v", kv.me, kv.gid, shardsInfo)
	return &CommandResponse{OK, ""}
}
