
#include "Client.h"
#include "serverRPC.pb.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>


bool RPCChan::isLeader(const ser::isLeaderRequest&args,ser::isLeaderResponse *response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(3));
    //调用RPC
    const grpc::Status status = stub_->isLeader(&context,args,response);
    if (!status.ok()) {
        std::cerr << "RPC failed: "
              << status.error_message() << '\n';
        return false;
    }
    return true;
}
bool RPCChan::execute(const ser::executeRequest&args,ser::executeResponse *response) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now()+std::chrono::seconds(3));
    //调用RPC
    const grpc::Status status = stub_->execute(&context,args,response);
    if (!status.ok()) {
        std::cerr << "RPC failed: "
              << status.error_message() << '\n';
        return false;
    }
    return true;
}
void Client::connect() {
    std::unique_lock<std::mutex> lck(mu_);
    while(is_connect_ == false)
    {
        lck.unlock();
        for(auto & [id,chan]:chans_)
        {
            std::thread th([this,chan = chan]{
                ser::isLeaderRequest args;
                ser::isLeaderResponse res;
                bool su = chan->isLeader(args,&res);
                if(su && res.is_leader())
                {
                    {
                        std::lock_guard<std::mutex> lc(mu_);
                        is_connect_ = true;
                        leader_chan_ = chan;
                        cond_.notify_all();
                        std::cout <<"[CONNECT]:SUCCESS" << std::endl;
                    }
                }
            });
            th.detach();
        }
        lck.lock();
        cond_.wait_for(lck,std::chrono::milliseconds(2000),[this](){
            return is_connect_ == true;
        });
    }
}
void Client::start() {
    //创建连接
    //构建连接
    for(const auto[id,node]:nodes_)
    {
        chans_[id] = std::make_unique<RPCChan>(nodes_[id]);
    }
    //连接主节点
    connect();
    //用户循环
    userLoop();

}
void Client::userLoop() {
    while(true)
    {
        //打印提示信息
        std::cout << "输入put/get/del key value" << std::endl;
        //接收用户输入
        std::string op;
        std::string key;
        std::string value;
        std::cin >> op;
        if(op =="get" || op == "del")
        {
            std::cin >> key ;
        }else if(op == "put")
        {
            std::cin >> key >> value;
        }
        else{
            std::cerr << "[ERROR]:非法命令" ;
            continue;
        }
        //执行命令
        ser::executeRequest args;
        ser::executeResponse res;

        while(true){
            bool su = leader_chan_->execute(args,&res);
            //根据响应,判断是否重连
            if(su == false)
            {
                std::cerr<<"[ERROR]:对端超时,重试中";
                continue;
            }else{
                //重试命令
                if(res.is_leader() ==false)
                {
                    is_connect_ = false;
                    connect();
                    continue;;
                }else{
                    if(res.success())
                    {
                        //执行成功
                        break;
                    }else{
                        std::cerr << "[ERROR]:执行失败";
                        continue;
                    }
                }
            }
        }   
        //返回响应给用户
        if(op=="get")
        {
            std::cout << "[GET]:" << key << "->" << res.value() << std::endl;
        }else if(op=="put")
        {
            std::cout << "[PUT]:" <<key << " " << value << std::endl;
        }else if(op=="del")
        {
            std::cout << "[DEL]:" << key << "->" << res.value() << std::endl;
        }
    }
}