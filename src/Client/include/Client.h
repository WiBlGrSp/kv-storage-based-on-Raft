#pragma once
#include <condition_variable>
#include <mutex>
#ifndef CLIENT_H
#define CLIENT_H
//RPC通道
#include "server_rpc.grpc.pb.h"
#include "server_rpc.pb.h"
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

//定义RPC客户端
class RPCChan
{
private:
    std::unique_ptr<ser::serverRPC::Stub>stub_;  //存根
public:
    explicit RPCChan(const std::string& address)
        : stub_(ser::serverRPC::NewStub(
              grpc::CreateChannel(
                  address,
                  grpc::InsecureChannelCredentials()))) {}
    bool isLeader(const ser::isLeaderRequest&args,ser::isLeaderResponse *response);
    bool execute(const ser::executeRequest&args,ser::executeResponse *response);
};
//客户端类
class Client
{
private:
    //服务端节点地址结构
    std::map<int,std::string>nodes_;
    //服务端节点RPC通道
    std::map<int,std::shared_ptr<RPCChan>> chans_;
    //当前主节点通道
    std::shared_ptr<RPCChan> leader_chan_=nullptr;
    //是否连接到主节点
    bool is_connect_=false;
    std::condition_variable cond_;
    std::mutex mu_;
public:
    Client(std::map<int,std::string>nodes):nodes_(nodes){}
    //广播isLeader请求,直到找到主节点
    void connect();
    //启动客户端
    void start();
    //主循环:接收用户输入,发送请求,接收响应,回传用户
    void userLoop();

};

#endif //!CLIENT_H
