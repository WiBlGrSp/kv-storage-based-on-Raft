#pragma once

#ifndef RAFTSTORAGE_H
#define RAFTSTORAGE_H
//定义基于raft的分布式存储服务
#include"Storage.h"
#include "KVStore.h"
#include "raft.h"
class RaftStorage :public Storage
{
    //raft核心引用
    RaftNode& raft_node_;
    //kv状态机引用
    KVStore& state_machine_;

public:

    RaftStorage(RaftNode&raft_node,KVStore &kv_store):Storage(),raft_node_(raft_node),state_machine_(kv_store){}
//实现Storage接口
public:
    bool put(const std::string&key,const std::string &value)override;
    //获取键对应的值,value=''表示该键不存在
    //return : 成功获取返回true,获取失败返回false
    bool get(const std::string&key,std::string&value)override;

};


#endif //!RAFTSTORAGE_H