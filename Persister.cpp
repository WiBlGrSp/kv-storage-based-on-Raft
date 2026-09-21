
#include "Persister.h"
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <sstream>
Persister::Persister(int id) {
    file_raft_state_ = prefix_raft_state_ + std::to_string(id) + ".txt";
    file_raft_log_ = prefix_raft_log_ + std::to_string(id) + ".txt";
}

bool Persister::saveRaftState(const std::string &data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ofstream ofs;
    ofs.open(file_raft_state_+".tmp",std::ios::trunc|std::ios::out);
    if(!ofs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    ofs << data;
    ofs.close();
    rename((file_raft_state_+".tmp").c_str(),file_raft_state_.c_str());
    return true;
}

bool Persister::readRaftState(std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ifstream ifs;
    ifs.open(file_raft_state_,std::ios::in);
    if(!ifs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    data = std::move(buffer.str());
    return true;
}

bool Persister::saveRaftLog(const std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ofstream ofs;
    ofs.open(file_raft_log_+".tmp",std::ios::trunc|std::ios::out);
    if(!ofs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    ofs << data;
    ofs.close();
    rename((file_raft_log_+".tmp").c_str(),file_raft_log_.c_str());
    return true;
}

bool Persister::readRaftLog(std::string&data) {
    std::lock_guard<std::mutex> lck(mtx_);
    std::ifstream ifs;
    ifs.open(file_raft_log_,std::ios::in);
    if(!ifs.is_open()){
        printf("error: open_file failure!!!\n");
        return false;    
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    data = std::move(buffer.str());
    return true;
}
// int main()
// {
//     Persister p(1);
//     p.saveRaftLog("nihao\nma");
//     p.saveRaftState("hhahaha\nsss\nzzz");
//     std::string data;
//     p.readRaftLog(data);
//     std::cout << data;
//     p.readRaftState(data);
//     std::cout << data;
//     return 0;
// }