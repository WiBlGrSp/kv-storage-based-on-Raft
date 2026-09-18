#pragma once
#include <random>
#ifndef RAFT_H
#define RAFT_H
#include <condition_variable>
#include <map>
#include "raftRPC.pb.h"
#include<string>
#include"RaftRPCClient.h"
//节点基本信息
struct Node{
    bool connect_;
    std::string address_;
    Node(const std::string&address):address_(address){
        connect_ = true;
    }
};
//节点状态枚举
enum class State{
    Follower=0,
    Candidate,
    Leader
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
    //本节点id
    int me_;    
    //除本节点外其他节点信息
    std::map<int,Node> nodes_;
    //本节点状态 
    State state_;
    //当前任期   
    int term;   
    //当前获得票数
    int vote_count_;
    //本轮投票所给节点id, 若没投票则为-1
    int vote_for_;  
    //接收到心跳包
    bool is_heartbeat_;
    //选举成功
    bool is_election_success_;

    std::condition_variable cond_heartbeat_;
    std::mutex mu_heartbeat_;

    std::condition_variable cond_election_success_;
    std::mutex mu_election_success_;
    //除本节点外其他RPC客户端
    std::map<int,std::shared_ptr<RaftRPCClient>> clis;
    //随机数发生器
    std::mt19937_64 rng_;

    //日志复制相关状态
    //日志容器
    std::vector<Entry> log_; 
    //上次提交序号
    int commited_index_;
    //上次应用到状态机序号
    int applied_index_;
    //保存发送给每个节点的下一条记录序号
    std::map<int,int>next_indexs_;
    //保存已经复制给每个节点的最后一条记录序号
    std::map<int,int>match_indexs_;

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
private:
    //日志相关状态初始化
    void logInit();
    int  getLastIndex()
    {
        if(log_.size()==0)
        {
            return 0;
        }
        return log_.back().index;
    }
    //模拟客户端定期追加日志
    void cliLike();
private:

    void followerRun();
    void candidateRun();
    void leaderRun();
    void broadcastRequestVote();
    void sendRequestVote(int id,const raft::VoteRequest& request,raft::VoteReply* response);
    void broadcastHeartBeat();
    void sendHeartBeat(int id,const raft::HeartBeatRequest &request,raft::HeartBeatReply*response);       
public:
    void onHeartBeat(const raft::HeartBeatRequest* request,raft::HeartBeatReply* response);
    void onRequestVote(const raft::VoteRequest* request,raft::VoteReply* response);
public:
    RaftNode(int id,const std::map<int,Node> nodes):me_(id),nodes_(nodes),rng_(MakeRng(id)){
        nodes_.erase(id);
    }
    ~RaftNode(){

    }
    //开启raft节点
    void start();
};


#endif //!RAFT_H