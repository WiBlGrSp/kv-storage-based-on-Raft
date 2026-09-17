#include <grpcpp/grpcpp.h>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <iostream>
#include <memory>
#include <string>

#include "greeter.grpc.pb.h"
#include "greeter.pb.h"

namespace {

class GreeterServiceImpl final : public grpc_example::Greeter::Service {
public:
    grpc::Status SayHello(grpc::ServerContext*,
                          const grpc_example::HelloRequest* request,
                          grpc_example::HelloReply* response) override {
        response->set_message("Hello, " + request->name());
        return grpc::Status::OK;
    }
    grpc::Status SayHelloAgain(grpc::ServerContext*,
                               const grpc_example::HelloRequest*request,
                               grpc_example::HelloReply*response)override{
        response->set_message("hello agains"+request->name());
        return grpc::Status::OK;

    }
};

void RunServer(const std::string& address) {
    GreeterServiceImpl service;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(
        address,
        grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "failed to start gRPC server\n";
        return;
    }

    std::cout << "gRPC server listening on " << address << '\n';
    server->Wait();
}

}  // namespace

int main(int argc, char** argv) {
    const std::string address =
        argc > 1 ? argv[1] : "0.0.0.0:50051";

    RunServer(address);
    return 0;
}
