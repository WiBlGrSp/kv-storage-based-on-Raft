#pragma once
#ifndef RAFTRPC_H
#define RAFTRPC_H
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <string>

#include "raftRPC.grpc.pb.h"
#include "raftRPC.pb.h"
class RaftNode;
//定义raftRPC服务

class RaftRPCServiceImpl final : public raft::RaftRPC::Service {
private:
    RaftNode& raft_node_;
public:
    RaftRPCServiceImpl(RaftNode&raft_node):raft_node_(raft_node){}
    //处理心跳包请求
    grpc::Status OnHeartBeat(grpc::ServerContext*,
                          const raft::HeartBeatRequest* request,
                          raft::HeartBeatReply* response) override;
    //处理要票请求
    grpc::Status OnRequestVote(grpc::ServerContext*,
                          const raft::VoteRequest* request,
                          raft::VoteReply* response) override;
};
//定义RPC服务器
class RaftServer
{
private:

    std::unique_ptr<grpc::Server> server_;
public:
    void start(const std::string&address,RaftNode&raft_node);
};



#endif //!RAFTRPC_H