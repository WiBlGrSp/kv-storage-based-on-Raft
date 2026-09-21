#pragma once
#include <fstream>
#include <mutex>
#ifndef PERSISTER_H
#define PERSISTER_H
//定义Raft的持久化模块
/*
    保存与恢复
保存状态到raftstate文件
保存日志到raftlog文件
从raftstate中读取状态
从raftlog中读取日志

*/
class Persister
{
private:
    std::mutex mtx_;
    const std::string prefix_raft_state_ = "raft-state";
    const std::string prefix_raft_log_ = "raft-log";
    std::string file_raft_state_;
    std::string file_raft_log_;
public:
    //根据id设置文件名
    Persister(int id);
    bool saveRaftState(const std::string &data);
    bool readRaftState(std::string&data);
    bool saveRaftLog(const std::string&data);
    bool readRaftLog(std::string&data);
};


#endif //!PERSISTER_H