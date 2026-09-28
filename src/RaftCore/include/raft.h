#pragma once
#include <mutex>
#ifndef RAFT_H
#define RAFT_H
#include "KVStore.h"
#include <future>
#include <memory>
#include <random>
#include <utility>
#include <condition_variable>
#include <map>
#include "raftRPC.pb.h"
#include<string>
#include"RaftRPCClient.h"
#include"Persister.h"
//节点基本信息
struct Node{
    bool connect_;
    std::string address_;
    Node(){};
    Node(const std::string&address):address_(address){
        connect_ = true;
    }
};
typedef std::string CMD;
//日志记录结构
struct Entry
{
    int index;  //序号
    int term;   //任期
    CMD cmd;    //命令
};
//raft节点定义
class RaftNode{
private:
    using myPromise = std::promise<bool>;
    using myFuture = std::future<bool>;
private:
    //节点状态枚举
    enum class State{
        Follower=0,
        Candidate,
        Leader
    };
private:
    //本节点id
    int me_;    
    //除本节点外其他节点信息
    std::map<int,Node> peers_;
    //本节点状态 
    State state_;
    //当前任期   
    int current_term_;   
    //当前获得票数
    int vote_count_;
    //本轮投票所给节点id, 若没投票则为-1
    int vote_for_;  
    // //接收到心跳包
    // bool is_heartbeat_;
    // //选举成功
    // bool is_election_success_;

    //定时资源
    std::mutex mu_;
    std::condition_variable cond_;
    std::chrono::steady_clock::time_point deadline_;    //超时时间点
    // std::mutex mu_heartbeat_;

    // std::condition_variable cond_election_success_;
    // std::mutex mu_election_success_;
    //除本节点外其他RPC客户端
    std::map<int,std::shared_ptr<RaftRPCClient>> clis;
    //随机数发生器
    std::mt19937_64 rng_;

    //日志复制相关状态
    //日志容器,序号从1开始
    std::vector<Entry> log_; 
    //上次提交序号
    int commited_index_;
    //上次应用到状态机序号
    int last_applied_;
    //保存发送给每个节点的下一条记录序号
    std::map<int,int>next_indexs_;
    //保存已经复制给每个节点的最后一条记录序号
    std::map<int,int>match_indexs_;
private:
    Persister persis_;  //持久化模块
private:
    std::map<std::pair<int,int>,std::shared_ptr<std::promise<bool>>> pendding_map_;   //外部请求等待
    KVStore &state_machine_;
private:
    //初始化随机数发生器
    std::mt19937_64 MakeRng(int node_id) {
    std::random_device rd;

    std::seed_seq seed{
        rd(),
        rd(),
        rd(),
        rd(),
        static_cast<unsigned int>(node_id),
        static_cast<unsigned int>(::getpid()),
    };

    return std::mt19937_64(seed);
    }
//日志容器函数
private:
    //日志相关状态初始化
    void logInit();
    int  getLastIndex()
    {
        if(log_.size()<=1)
        {
            return 0;
        }
        return log_.back().index;
    }
    int getLastTerm()
    {
        if(log_.size() <= 1)
        {
            return 0;
        }
        return log_.back().term;
    }
    //传入日志更新或一致,返回true
    bool newerOrEqualLogs(int term,int index){
        if(this->getLastTerm() < term)
            return true;
        else if(this->getLastTerm() > term)
            return false;
        if(this->getLastIndex() > index)
            return false;
        return true;
    }
    //模拟客户端定期追加日志
    void cliLike();
//持久化函数
private:
    //保存current_term_和vote_for
    bool saveState();
    //加载current_term_和vote_for
    bool loadState();   
    //保存日志
    bool saveLog();
    //加载日志
    bool loadLog();
    //提交日志
    bool commitLog(int index);
    //检查,更新commit_index并提交
    bool updateCommit();
//主干函数 和 RPC请求封装
private:
    void followerRun();
    void candidateRun();
    void leaderRun();
    void broadcastRequestVote();
    void sendRequestVote(int id,const int term_snapshot);
    void broadcastHeartBeat();
    void sendHeartBeat(int id,const int term_snapshot);       
private:
    // void resetHeartBeatTimer();
    //应用日志到状态机
    void applyLoop();
//RPC响应封装
public:
    void onHeartBeat(const raft::HeartBeatRequest* request,raft::HeartBeatReply* response);
    void onRequestVote(const raft::VoteRequest* request,raft::VoteReply* response);
public:
    RaftNode(int id,const std::map<int,Node> nodes,KVStore&kv_store)
    :me_(id),peers_(nodes),rng_(MakeRng(id)),persis_(id),state_machine_(kv_store){
        peers_.erase(id);
        this->log_.push_back(Entry{});
    }
    ~RaftNode(){

    }
    //开启raft节点
    void start();
private:
    //定时器函数
    void ticker();
    //重置选举定时器
    void resetElectionTimerLocked();
    //重新心跳定时器
    void resetHeartBeatTimerLocked();
    //选举超时行为
    void onElectionTimeout();
    //心跳超时行为
    void onHeartBeatTimeout();
public:
    //外部调用,请求添加日志
    myFuture propose(const std::string&cmd);
};


#endif //!RAFT_H