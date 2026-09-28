#pragma once
#ifndef RPCSERVER_H
#define RPCSERVER_H
#include"serverRPC.grpc.pb.h"
#include"serverRPC.pb.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
class Server;
//RPC服务器类实现
//RPC服务实现
class ServerRPCServiceImpl final : public ser::serverRPC::Service {
private:
    Server*server_;
public:
    ServerRPCServiceImpl(Server*server):server_(server){}
    grpc::Status isLeader(grpc::ServerContext*,
                          const ser::isLeaderRequest* request,
                          ser::isLeaderResponse* response) override;
    grpc::Status execute(grpc::ServerContext*,
                          const ser::executeRequest* request,
                          ser::executeResponse* response) override;
};
#endif //!RPCSERVER_H