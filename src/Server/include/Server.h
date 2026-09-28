#pragma once

#ifndef SERVER_H
#define SERVER_H
#include"serverRPC.pb.h"
#include "KVStore.h"
#include "raft.h"
#include <grpcpp/server.h>
#include"Storage.h"

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
    bool isLeader(const ser::isLeaderRequest* request,ser::isLeaderResponse* response);
    bool execute(const ser::executeRequest*request, ser::executeResponse*response);
    //启动服务器
    void start();
    //启动RPC服务
    void startRPC();
};

#endif //!SERVER_H