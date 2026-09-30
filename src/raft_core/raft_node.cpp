#include"raft_node.h"
#include <cassert>
#include <chrono>
#include <cstdlib>
#include "kv_store.h"
#include "raft_rpc_client.h"
#include "raft_rpc.pb.h"
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include<thread>
#include <tuple>
#include <utility>
void RaftNode::Start() {
    {
        std::lock_guard<std::mutex> lck1(mu_);
        //初始化节点信息
        state_ = State::kFollower;
        current_term_ = 0;
        vote_count_ = 0;
        vote_for_ = -1;
        commited_index_ = 0;
        last_applied_ = 0;
        //加载持久化信息
        LoadState();
        LoadLog();
        //构建连接
        for(const auto[id,node]:peers_)
        {
            clis_[id] = std::make_unique<RaftRPCClient>(node.address);
        }
        //状态机应用
        thread_apply_loop_ = std::make_unique<std::thread>(&RaftNode::ApplyLoop,this); 
    }
    //启动定时线程
    Ticker();
    // while(true)
    // {
    //     switch (state_) {
    //         case State::Follower:
    //             followerRun();
    //         break;
    //         case State::Candidate:
    //             candidateRun();
    //         break;
    //         case State::Leader:
    //             leaderRun();
    //         break;
    //     }
    // }
}

// void RaftNode::followerRun() {

//     {
//         std::unique_lock<std::mutex> lck(mu_);
//         is_heartbeat_ = false;
//         //设置心跳定时器,定时为100~200ms
//         auto dest = std::chrono::steady_clock::now()+std::chrono::milliseconds(rng_()%300+500);
//         cond_.wait_until(lck,dest,[this]()->bool{
//             return is_heartbeat_ == true;
//         });   
//         //过期且未收到心跳包
//         if(!is_heartbeat_)
//         {
//             state_ = State::Candidate;
//             return;
//         }
//     }
// }
// void RaftNode::candidateRun() {

//     std::unique_lock<std::mutex> ulck(mu_);
//     //修改任期
//     current_term_ ++;
//     //为自己投票
//     vote_count_ =1;
//     vote_for_ = me_;
//     saveState();
//     printf("term=%d:参与选举\n",this->current_term_);


//     ulck.unlock();
//     //广播要票请求
//     broadcastRequestVote();
//     //启动选举定时器,过期则降为follower
//     ulck.lock();
//     is_election_success_ = false;
//     //设置选举定时器,定时为500~1000ms
//     auto dest = std::chrono::steady_clock::now()+std::chrono::milliseconds( 300+rng_()%(5000-300));
//     cond_.wait_until(ulck,dest,[this]()->bool{
//         return is_election_success_ == true || this->state_ !=State::Candidate;
//     });
//     ulck.lock();
//     //过期且未选举成功
//     if(this->state_ !=State::Candidate)
//     {
//         return;
//     }
//     if(!is_election_success_)
//     {
//         printf("选举失败\n");
//         return;
//     }else
//     {
//         printf("选举成功\n");
//         //选举成功
//         state_ = State::Leader;
//         //初始化日志状态
//         logInit();
//         // //模拟客户端定期发送日志
//         // cliLike();
//         return;
//     }
// }
// void RaftNode::leaderRun() {

//     //每隔100ms广播心跳包
//     broadcastHeartBeat();       
//     std::this_thread::sleep_for(std::chrono::milliseconds(100));
// }
void RaftNode::OnHeartBeat(const raft::HeartBeatRequest* request,raft::HeartBeatReply* response) {
    
    //TODO:如果有日志,则进行日志处理
    //1.判断"前一条日志"是否匹配,如果匹配,进行追加
    //2.如果不匹配,更新reply中的nextIndex,告知leader下次发送

    std::unique_lock<std::mutex> lck1(mu_);
    if(this->current_term_ < request->term())
    {
        //如果leader任期更新,则修改当前节点任期
        this->current_term_ = request->term();
        this->vote_for_ = -1;
        this->state_ = State::kFollower;
        SaveState();
    }else 
    if(this->current_term_> request->term())
    {
        //如果leader任期更旧,不做处理
        response->set_term(this->current_term_);
        response->set_success(false);
        return;
    }
    //同任期
    response->set_term(this->current_term_);
    this->state_ = State::kFollower;

    //心跳成功,重置选举定时器
    ResetElectionTimerLocked();
    
    //检查日志匹配性
    //如果follower没有prevIndex日志,或者preIndex日志处不匹配,不做处理,响应false
    int prev_index = request->prev_index();
    
    if(this->GetLastIndex() < prev_index || this->log_.at(prev_index).term != request->prev_term())
    {
        response->set_success(false);
    }else //从节点匹配成功
    {
        //如果心跳携带日志
        if(request->entries_size()>0)
        {
            //删除匹配点之后的日志
            auto begin = this->log_.begin()+prev_index+1;
            if(begin>=this->log_.begin() && begin < this->log_.end())
                this->log_.erase(begin,this->log_.end());
            else
                std::cerr << "[ERROR]:log index overflow";        
            //追加新日志
            auto entries = request->entries();
            for(const auto kE:entries)
            {
                this->log_.emplace_back(Entry{kE.index(),kE.term(),kE.cmd()});
            }
            //持久化日志
            SaveLog();
            std::cout << "追加成功,当前日志为:" << std::endl;
            for(const auto kS : log_)
            {
                std::cout << "(" << kS.index << ',' << kS.term <<')' << kS.cmd << std::endl;
            }
        }
        //提交序号更新
        if(commited_index_ < request->commited_index())
        {
            commited_index_ = std::min(GetLastIndex(),request->commited_index());
        }
        //响应成功
        response->set_success(true);
    }
}
//广播要票请求
void RaftNode::BroadcastRequestVote() {

    //任期快照
    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_);
        term_snapshot = this->current_term_;
    }
    for(const auto&[id,node]:peers_)
    {
        thread_pool_->AddTask([this,id = id,term_snapshot](){
            SendRequestVote(id,term_snapshot);
        });
    }
}
void RaftNode::SendRequestVote(int id,const int kTermSnapshot)
{
    raft::VoteRequest request;
    raft::VoteReply response;
    std::unique_lock<std::mutex> lck(mu_);
    if(!(state_ == State::kCandidate && this->current_term_ == kTermSnapshot))
    {
        return;
    }
    request.set_candidate_id(this->me_);
    request.set_term(this->current_term_);
    //candidate发送日志追加情况
    request.set_last_log_term(GetLastTerm());
    request.set_last_log_index(GetLastIndex());
    lck.unlock();
    bool res = clis_[id]->SendRequestVote(request,&response);
    if(!res)    return;
    // printf("Vote:id=%d,term=%d\n",id,request.term());
    // printf("VoteRes:granted=%d,term=%d\n",response->vote_granted(),response->term());
    lck.lock();
    if(this->current_term_ < response.term())
    {
        this->current_term_ = response.term();
        vote_for_ = -1;
        state_ = State::kFollower;
        SaveState();
        ResetElectionTimerLocked();
        return;
    }
    if(!(state_ == State::kCandidate && this->current_term_ == kTermSnapshot))
    {
        return;
    }
 
    if(response.vote_granted() && this->state_ == State::kCandidate && response.term() == request.term())
    {
        this->vote_count_++;
        //选举成功
        if(this->vote_count_>=(this->peers_.size()+1)/2+1)
        {
            std::cout<< me_ << "is leader" << std::endl; 
            this->state_ = State::kLeader;
            //追加no-op日志用于同步
            this->log_.emplace_back(Entry{GetLastIndex()+1,this->current_term_,"no-op"});
            LogInit();
            ResetHeartBeatTimerLocked();
        }
    }
}

//广播心跳包
void RaftNode::BroadcastHeartBeat() {
    //对每个结点判断是否有可发日志,并封装日志信息
    //任期快照
    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_);
        term_snapshot = this->current_term_;
    }
    for(const auto&[id,_] : peers_)
    {
        thread_pool_->AddTask([this,id = id,term_snapshot](){
            SendHeartBeat(id,term_snapshot);
        });
    }
}
void RaftNode::SendHeartBeat(int id,const int kTermSnapshot)
{
    raft::HeartBeatRequest request;
    raft::HeartBeatReply response;
    std::unique_lock<std::mutex>lck1(mu_);
    //封装请求包
    if(!(this->state_ == State::kLeader && this->current_term_ ==kTermSnapshot))
    {
        return;
    }
    request.set_leader_id(me_);
    request.set_term(current_term_);
    request.set_commited_index(this->commited_index_);
    int prev_index = this->next_indexs_[id]-1;
    request.set_prev_index(prev_index);
    request.set_prev_term(this->log_[prev_index].term);
    //还有记录未发送,打包发送

    if(GetLastIndex() >= this->next_indexs_[id])
    {
        for(auto it = this->log_.begin()+prev_index+1;it!=this->log_.end();it++)
        {
            auto e = request.add_entries();
            e->set_index(it->index);
            e->set_term(it->term);
            e->set_cmd(it->cmd);
        }
    }

    lck1.unlock();
    //数据传输
    bool res = clis_[id]->SendHeartBeat(request,&response);
    if(!res)    return;
    lck1.lock();
    //接收并解析响应
    if(!(this->state_ == State::kLeader && this->current_term_ ==kTermSnapshot))
    {
        return;
    }

    //如果自己任期更旧,降至follower状态
    if(this->current_term_ < response.term())
    {
        this->current_term_ = response.term();
        vote_for_ = -1;
        state_ = State::kFollower;
        SaveState();
        ResetElectionTimerLocked();
        return;
    }

    //根据从节点响应,分支

    if(response.success())
    {
        //如果心跳成功
        if(request.entries_size()>0)
        {
            //更新match和next
            match_indexs_[id] = next_indexs_[id] + request.entries_size()-1;    //发送日志组最后一条记录的序号
            next_indexs_[id] = match_indexs_[id] +1;
            //TODO:检查commit,更新commitIndex,并提交
            UpdateCommit();
        }
    }else
    {
        //如果心跳失败,递减next,下一次心跳重试
        next_indexs_[id] --;
    }

    
}
    
    

void RaftNode::OnRequestVote(const raft::VoteRequest* request,raft::VoteReply* response) {
    std::lock_guard<std::mutex> lck(mu_);
    bool granted = false;   //是否投票
    //任期更旧,更新任期状态
    if(this->current_term_ < request->term())
    {
        this->current_term_ = request->term();
        this->vote_for_ = -1;
        this->state_ = State::kFollower;
        SaveState();
        ResetElectionTimerLocked();
    }
    //如果任期一致,follower未投票,candidate至少比follower新或一致,则投票
    if(this->current_term_ == request->term() && this->vote_for_ ==-1 && NewerOrEqualLogs(request->last_log_term(),request->last_log_index()))
    {
        granted = true;
    }else{
        granted = false;
    }
    //投票,更新心跳定时器
    if(granted)
    {
        this->vote_for_ = request->candidate_id();
        SaveState();
        response->set_vote_granted(true);
        response->set_term(this->current_term_);
        ResetElectionTimerLocked();
        printf("term=%d:投票给%d\n",this->current_term_,this->vote_for_);
    }else
    {
        response->set_vote_granted(false);
        response->set_term(this->current_term_);
    }
}
void RaftNode::LogInit() {
    // commited_index_ = 0;
    // last_applied_ = 0;
    for(const auto&[id,v]:peers_)
    {
        next_indexs_[id] = GetLastIndex()+1;
        match_indexs_[id] = 0;
    }
}
void RaftNode::CliLike() {
    std::thread th([this](){
        int i = this->GetLastIndex();
        while(this->state_==State::kLeader)
        {
            i++;
            this->log_.emplace_back(Entry{i,this->current_term_,"test_"+std::to_string(i)});
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    });
    th.detach();
}

//必须外部持有锁
bool RaftNode::SaveState() {
    std::string data;
    data = std::to_string(current_term_) + ":" + std::to_string(vote_for_);
    return persis_.SaveRaftState(data);
}
bool RaftNode::LoadState() {
    std::string data;
    bool res = persis_.ReadRaftState(data);
    if(res == false)
        return false;
    auto it = data.find(":");
    this->current_term_ = std::stoi(data.substr(0,it));
    this->vote_for_ = std::stoi(data.substr(it+1));
    printf("loadState :%d:%d\n",current_term_,vote_for_);
    return true;
}
//必须外部持有锁
bool RaftNode::SaveLog() {

    std::stringstream ss;
    int size = log_.size();
    for(size_t i = 1;i<size;i++)
    {
        ss << log_[i].index << ":" << log_[i].term << ":" << log_[i].cmd << "\n";
    }
    return persis_.SaveRaftLog(ss.str());
}
bool RaftNode::LoadLog() {
    std::string data;
    persis_.ReadRaftLog(data);
    std::stringstream ss(std::move(data));
    std::string line;
    while(std::getline(ss,line))
    {
        auto it1 = line.find(':');
        auto it2 = line.find(':',it1+1);
        int index = std::stoi(line.substr(0,it1));
        int term = std::stoi(line.substr(it1+1,it2-it1-1));
        std::string cmd = line.substr(it2+1);
        this->log_.emplace_back(Entry{index,term,std::move(cmd)});
        printf("loadLog:%d:%d:%s\n",index,term,cmd.c_str());
    }
    return true;
}
bool RaftNode::CommitLog(int index)
{
    std::stringstream ss;
    int size = log_.size();
    for(size_t i = 1;i<=index;i++)
    {
        ss << log_[i].index << ":" << log_[i].term << ":" << log_[i].cmd << "\n";
    }
    return persis_.SaveRaftLog(ss.str());
}
bool RaftNode::UpdateCommit()
{
    //搜索最大的多数match序号res
    int left = commited_index_ +1;
    int right = GetLastIndex();
    int mid;
    int count;
    if(left >right)
    {
        return false;
    }

    while(left <=right)
    {
        mid = (left+right)/2;
        count = 1;
        //遍历match,统计数目
        for(const auto [id,match] : match_indexs_)
        {
            if(mid <=match)
                count++;
        }
        if(count >= (peers_.size()+1)/2+1)
        {
            left = mid + 1;
        }else {
            right = mid-1;
        }
    }
    int to_commit_index;
    if(count >= (peers_.size()+1)/2+1)
    {
        to_commit_index = mid;
    }else{
        to_commit_index = mid-1;
    }   
    //保证commitindex有改变,且提交日志为本任期,再进行提交
    if(to_commit_index!=commited_index_ && this->log_[to_commit_index].term == this->current_term_){
        // //提交日志,更新commit
        // if(CommitLog(to_commit_index))
        // {
            commited_index_ = to_commit_index;
        // }
    }

    return true;
}
RaftNode::MyFuture RaftNode::Propose(const std::string&cmd)
{
    std::lock_guard<std::mutex> lck(mu_);
    //不是领导者直接退出
    if(this->state_ !=State::kLeader)
    {
            MyPromise p;
            p.set_value(false);
            return p.get_future();
    }
    //追加日志
    int index = GetLastIndex()+1;
    int term = this->current_term_;
    this->log_.emplace_back(Entry{index,term,cmd});
    //持久化日志
    this->SaveLog();
    //将客户端请求添加到pendding,等待commit与apply成功
    auto p = std::make_shared<MyPromise>();
    this->pendding_map_[std::pair<int, int>(index,term)] = p;
    return p-> get_future();
}
void RaftNode::ApplyLoop()
{
    while(true)
    {
        {
            std::lock_guard<std::mutex> lck(mu_);
            while(last_applied_ < commited_index_)
            {
                int i = last_applied_+1;
                Entry &e = this->log_[i];
                //解析命令
                std::istringstream ss(e.cmd);
                std::string op;
                std::string key;
                std::string value;
                ss >> op >> key >> value;
                bool success = false;
                if(op == "put")
                {
                    state_machine_.Put(key,value);
                    std::cout << "[APPLY]:" << e.cmd << std::endl;
                    success = true;
                }
                else if(op == "del")
                {
                    state_machine_.Del(key);
                    std::cout << "[APPLY]:" << e.cmd << std::endl;
                    success = true;
                }
                auto index_for_pendding = std::make_pair(e.index,e.term);
                if(this->pendding_map_.find(index_for_pendding) !=pendding_map_.end())
                {
                    auto p = this->pendding_map_[std::make_pair(e.index,e.term)];
                    p->set_value(success);
                    this->pendding_map_.erase(std::make_pair(e.index,e.term));
                }
                last_applied_++;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void RaftNode::Ticker() {
    {
        std::lock_guard<std::mutex> lck(mu_);
        if(this->state_ == State::kLeader)
        {
            ResetHeartBeatTimerLocked();
        }else{
            ResetElectionTimerLocked();
        }
    }
    std::unique_lock<std::mutex> lck(mu_);
    while(true)
    {
        //等待超时或唤醒
        cond_.wait_until(lck,deadline_);
        //如果是超时
        auto now = std::chrono::steady_clock::now();
        if(now>=deadline_)
        {
            //触发超时
            switch (state_) {
                case State::kFollower:
                case State::kCandidate:
                    lck.unlock();
                    OnElectionTimeout();
                    lck.lock();
                    ResetElectionTimerLocked();
                    break;
                case State::kLeader:
                    lck.unlock();
                    OnHeartBeatTimeout();
                    lck.lock();
                    ResetHeartBeatTimerLocked();
                    break;
            }
        }else{
            continue;   //发生重置,重睡
        }
    }
}
void RaftNode::ResetElectionTimerLocked() {

    auto timeout = std::chrono::milliseconds(500+rng_()%500);

    if(this->state_ == State::kCandidate)
    {
        timeout +=  std::chrono::milliseconds(1000);
    }        
    deadline_ = std::chrono::steady_clock::now() + timeout;
    cond_.notify_all();
}
void RaftNode::ResetHeartBeatTimerLocked() {
    if(state_ == State::kLeader)
    {
        auto timeout = std::chrono::milliseconds(50);
        deadline_ = std::chrono::steady_clock::now()+timeout;
        cond_.notify_all();
    }
}
void RaftNode::OnElectionTimeout() {
    {
        std::lock_guard<std::mutex> lck(mu_);
        current_term_ ++;
        vote_count_ =1;
        vote_for_ = me_;
        this->state_ = State::kCandidate;
        SaveState();
        printf("term=%d:参与选举\n",this->current_term_);
    }
    //广播要票请求
    BroadcastRequestVote();
}
void RaftNode::OnHeartBeatTimeout() {
    BroadcastHeartBeat();
}
