
#include "include/ServerRPC.h"
#include"Server.h"
grpc::Status ServerRPCServiceImpl::isLeader(grpc::ServerContext*,
                          const ser::isLeaderRequest* request,
                          ser::isLeaderResponse* response) {
    if (server_!=nullptr)
    {
        server_->isLeader(request, response);
    }
    else
    {
        printf("server已经被释放\n");
    }
    return grpc::Status::OK;
}

grpc::Status ServerRPCServiceImpl::execute(grpc::ServerContext*,
                          const ser::executeRequest* request,
                          ser::executeResponse* response) {
    if (server_!=nullptr)
    {
        server_->execute(request, response);
    }
    else
    {
        printf("server已经被释放\n");
    }
    return grpc::Status::OK;
}

