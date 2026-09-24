
#include "include/RaftStorage.h"
#include <chrono>
#include <future>
#include <sstream>

bool RaftStorage::put(const std::string&key,const std::string &value) {

    //raft节点追加一条记录,返回future
    std::stringstream cmd;
    if(value == "")
    {
        cmd  << "del " <<key;
    }else{
        cmd << "put " << key <<' ' << value;
    }
    auto fu = raft_node_.propose(cmd.str());
    //异步:超时等待raft应用响应
    auto status = fu.wait_for(std::chrono::milliseconds(400));
    //超时或raft提交失败,返回false
    if(status == std::future_status::ready && fu.get() == true)
    {
        std::cout << "[SUCCESS]:put " << key << ' ' << value << std::endl;
        return true;
    }
    return false;    
}

bool RaftStorage::get(const std::string&key,std::string&value) {
    //从状态机获取键对应的值
    bool res = state_machine_.get(key,value);
    if(res == false)
        value="";
    else
        std::cout << "[SUCCESS]:get " << key << ' ' << value << std::endl;
    return res;
}
