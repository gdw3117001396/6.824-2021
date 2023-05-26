package shardkv

import "6.824/shardctrler"

// 配置更新
// 1.只有所有分片都是Serving才可以拉取配置
// 2.通过Query拉取当前配置Num的下一个配置
// 3.只能递增的更新配置
func (kv *ShardKV) PullConfig() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.kvStore {
		if shard.Status != Serving {
			canPerformNextConfig = false
			DPrintf("kvserver: %d , group: %d 不能拉取最新配置因为没有全部Serving, kv.currentConfig:%v", kv.me, kv.gid, kv.currentConfig)
			break
		}
	}
	kv.mu.Unlock()
	currentConfig := kv.currentConfig
	if canPerformNextConfig {
		nextConfig := kv.mck.Query(currentConfig.Num + 1)
		if currentConfig.Num+1 == nextConfig.Num {
			kv.Execute(NewPullConfigCommand(&nextConfig), &CommandResponse{})
		}
	}
}

// 配置更新，只能逐步递增的去更新配置
func (kv *ShardKV) applyPullConfigL(nextConfig *shardctrler.Config) *CommandResponse {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		DPrintf("kvserver: %d , group: %d 更新了配置信息", kv.me, kv.gid)
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandResponse{OK, ""}
	}
	DPrintf("kvserver: %d , group: %d 拒绝config", kv.me, kv.gid)
	return &CommandResponse{ErrOutDated, ""}
}

// 配置更新,更新全部分片状态,
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		// 这里的currentConfig其实是上一个配置
		// 该分片上一个配置不是自己负责而下一个配置是自己负责，则要去拉取数据
		if kv.currentConfig.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				kv.kvStore[i].Status = Pulling
			}
		}
		// 该分片上一个配置是自己负责而下一个配置不是自己负责，则要等待别人来拉取数据
		if kv.currentConfig.Shards[i] == kv.gid && nextConfig.Shards[i] != kv.gid {
			gid := nextConfig.Shards[i]
			if gid != 0 {
				kv.kvStore[i].Status = BePulling
			}
		}
	}
}
