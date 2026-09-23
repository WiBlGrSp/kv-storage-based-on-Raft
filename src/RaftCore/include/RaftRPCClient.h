#pragma once
#ifndef RAFTRPCCLIENT_H
#define RAFTRPCCLIENT_H

#include "raftRPC.grpc.pb.h"
#include "raftRPC.pb.h"
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

//定义RPC客户端
class RaftRPCClient
{
private:
    std::unique_ptr<raft::RaftRPC::Stub>stub_;  //存根
public:
    explicit RaftRPCClient(const std::string& address)
        : stub_(raft::RaftRPC::NewStub(
              grpc::CreateChannel(
                  address,
                  grpc::InsecureChannelCredentials()))) {}
    bool SendHeartBeat(const raft::HeartBeatRequest &request,raft::HeartBeatReply*response);     
    bool SendRequestVote(const raft::VoteRequest &request,raft::VoteReply*response);       

};


#endif //!RAFTRPCCLIENT_H