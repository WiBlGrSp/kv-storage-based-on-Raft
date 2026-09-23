#include "KVStore.h"
#include "raft.h"
#include"RaftRPC.h"
#include"RaftStorage.h"
#include <thread>
void test(Storage & storage,std::string op ,std::string key,std::string value)
{
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::string tip = "[CLI]:" + op + ' ' + key +' ' + value;
    std::cout << tip << std::endl;
    bool res;
    if(op == "put"){
        res = storage.put(key,value);
    }
    else{
        res = storage.get(key,value); 
    }
}
//要求输入--me=1@地址:端口 --peers=id@ip:port
int main(int argc,const char*argv[])
{
    printf("program start\n");
    int me;
    std::string address;
    std::map<int,Node> nodes;
    if(argc<2)
    {
        printf("too little arguments\n");
        return -1;
    }else{
        for(int i =1;i<argc;i++)
        {
            std::string buf(argv[i]);
            auto it = buf.find('=');
            auto it2 = buf.find('@');
            int id = stoi(buf.substr(it+1,it2-it-1));
            std::string addr = buf.substr(it2+1);
            if(i == 1)
            {
                me = id;
                address = addr;
            }else{
                nodes.emplace(id,Node(addr));
            }
        }
    }
    KVStore kv_store;
    RaftNode raft_node(me,nodes,kv_store);
    RaftServer raft_server;
    raft_server.start(address,raft_node);
    std::thread th([&](){
        raft_node.start();
    });
    th.detach();
    RaftStorage storage(raft_node,kv_store);
    while(true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        printf("模拟客户端调用storage接口\n");
        test(storage,"put","a","123");
        test(storage,"get","a","");
        test(storage,"put","a","");
        test(storage,"get","a","");
    }
    return 0;
}