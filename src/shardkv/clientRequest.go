package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := &Op{
		Key:       args.Key,
		Value:     "",
		Op:        OpGet,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	var response CommandResponse
	kv.Command(cmd, &response)
	reply.Err = response.Err
	reply.Value = response.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := &Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        OperationOp(args.Op),
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	var response CommandResponse
	kv.Command(cmd, &response)
	reply.Err = response.Err
}

func (kv *ShardKV) Command(request *Op, response *CommandResponse) {
	kv.mu.Lock()

	if request.Op != OpGet && kv.isduplicateSequenceNumL(request.ClientId, request.CommandId) {
		LastResponse := kv.lastOperations[request.ClientId].LastResponse
		response.Value = LastResponse.Value
		response.Err = LastResponse.Err
		kv.mu.Unlock()
		return
	}
	// 只有请求中的ShardId在shardManager内且为已就绪状态，服务端才能提供服务，不然返回ErrWrongGroup，让客户端重新拉取配置再重试请求。
	if !kv.canServeL(key2shard(request.Key)) {
		response.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.Execute(NewExecuteCommand(request), response)

}

// 读写操作
func (kv *ShardKV) applyExecuteL(operation *Op) *CommandResponse {
	var response *CommandResponse
	shardId := key2shard(operation.Key)
	if kv.canServeL(shardId) {
		if operation.Op != OpGet && kv.isduplicateSequenceNumL(operation.ClientId, operation.CommandId) {
			DPrintf("kvserver: %d , group: %d 不能提交重复信息 %v 因为 client: %v 最新的seqId 是 %v", kv.me, kv.gid, operation, operation.ClientId, kv.lastOperations[operation.ClientId].MaxAppliedCommandId)
			return kv.lastOperations[operation.ClientId].LastResponse
		} else {
			response = kv.applyLogToKvStoreL(operation, shardId)
			if operation.Op != OpGet {
				kv.lastOperations[operation.ClientId] = OperationContext{operation.CommandId, response}
			}
			return response
		}
	}
	return &CommandResponse{ErrWrongGroup, ""}
}

// 读写操作
func (kv *ShardKV) applyLogToKvStoreL(opertion *Op, shardID int) *CommandResponse {
	var value string
	var err Err
	switch opertion.Op {
	case OpPut:
		err = kv.kvStore[shardID].Put(opertion.Key, opertion.Value)
	case OpAppend:
		err = kv.kvStore[shardID].Append(opertion.Key, opertion.Value)
	case OpGet:
		value, err = kv.kvStore[shardID].Get(opertion.Key)
	}
	return &CommandResponse{err, value}
}
