#pragma once

#ifndef SERVER_H
#define SERVER_H
#include"server_rpc.pb.h"
#include "kv_store.h"
#include "raft_node.h"
#include <grpcpp/server.h>
#include"storage.h"

//服务器类
class Server
{
private:
    //服务器基本信息
    std::string address_;
    std::unique_ptr<grpc::Server> rpc_server_;
    int me_;
    std::map<int,std::string>peers_;
    //子模块
    RaftNode*raft_node_;
    KVStore*kv_store_;
    Storage* storage_;
public:
    Server(int me,const std::string&address,std::map<int,std::string>peers);
    ~Server();
    //提供给RPC的回调函数
    bool IsLeader(const ser::isLeaderRequest* request,ser::isLeaderResponse* response);
    bool Execute(const ser::executeRequest*request, ser::executeResponse*response);
    //启动服务器
    void Start();
    //启动RPC服务
    void StartRPC();
};

#endif //!SERVER_H
