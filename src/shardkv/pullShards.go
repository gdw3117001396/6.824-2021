package shardkv

import "sync"

// 拉取数据
// 1.整合出属于同一集群的shard，然后进行批量拉取，这样就不用为每个shard都发一次请求，减少对资源的消耗。
// 2. waitGroup 来保证所有独立地任务完成后才会进行下一次任务
func (kv *ShardKV) PullShards() {
	kv.mu.Lock()
	// 整合出属于同一集群的shard，然后进行批量拉取，这样就不用为每个shard都发一次请求，减少对资源的消耗。
	gid2shardIDs := kv.getShardIDsByStatusL(Pulling)
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		DPrintf("kvserver: %d , group: %d 开始拉取分片 shardIDs %d 从 group: %d", kv.me, kv.gid, shardIDs, gid)
		wg.Add(1)
		// 传lastConfig的对应服务器，currentConfig的configNum, shardIDs
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationArgs{configNum, shardIds}
			for _, server := range servers {
				var pullTaskResponse ShardOperationReply
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {
					DPrintf("kvserver: %d , group: %d 得到了pullTaskResponse %v", kv.me, kv.gid, pullTaskResponse)
					kv.Execute(NewPullShardsCommand(&pullTaskResponse), &CommandResponse{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}

// 拉取数据的RPC
// 1.首先仅可由 leader 处理该请求
// 2.其次如果发现请求中的配置版本大于本地的版本,那说明请求拉取的是未来的数据，则返回 ErrNotReady 让其稍后重试
// 3.拷贝对应的分片数据和拷贝整个去重表
func (kv *ShardKV) GetShardsData(request *ShardOperationArgs, response *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.currentConfig.Num < request.ConfigNum {
		response.Err = ErrNotReady
		return
	}
	response.Shards = make(map[int]map[string]string)
	for _, shardId := range request.ShardIDs {
		response.Shards[shardId] = kv.kvStore[shardId].deepCopy()
	}
	response.LastOperations = make(map[int64]OperationContext)
	for clientID, operation := range kv.lastOperations {
		response.LastOperations[clientID] = operation.deepCopy()
	}
	response.Err = OK
	response.ConfigNum = request.ConfigNum
}

// 计算对应status分片在lastConfig的gid, 整合到一起，返回gid->shardIDs
// 当前服务器应该去哪里拉取分片，或者去哪里告知可以删除分片了
func (kv *ShardKV) getShardIDsByStatusL(status ShardStatus) map[int][]int {
	gid2shardIDs := make(map[int][]int, 0)
	for i, shard := range kv.kvStore {
		if shard.Status == status {
			// 该分片在lastConfig中的gid
			gid := kv.lastConfig.Shards[i]
			if _, ok := gid2shardIDs[gid]; !ok {
				gid2shardIDs[gid] = make([]int, 0)
			}
			gid2shardIDs[gid] = append(gid2shardIDs[gid], i)
		}
	}
	return gid2shardIDs
}

// 1. 仅可执行与当前配置版本相同地分片更新日志，否则返回 ErrOutDated。
// 2. 仅在对应分片状态为 Pulling 时为第一次应用，此时覆盖状态机即可并修改状态为 GCing
// 3. 更新去重表
func (kv *ShardKV) applyPullShardsL(shardsInfo *ShardOperationReply) *CommandResponse {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("kvserver: %d , group: %d 接受 shards插入 %v", kv.me, kv.gid, shardsInfo)
		for shardId, shardData := range shardsInfo.Shards {
			shard := kv.kvStore[shardId]
			if shard.Status == Pulling {
				for key, value := range shardData {
					shard.KV[key] = value
				}
				shard.Status = GCing
			} else {
				break
			}
		}

		for clientId, operationContext := range shardsInfo.LastOperations {
			if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.MaxAppliedCommandId < operationContext.MaxAppliedCommandId {
				kv.lastOperations[clientId] = operationContext
			}
		}
		return &CommandResponse{OK, ""}
	}
	DPrintf("kvserver: %d , group: %d 拒绝outdated的 shards插入 %v", kv.me, kv.gid, shardsInfo)
	return &CommandResponse{ErrOutDated, ""}
}
