#include <chrono>
#include <cstdlib>
#include"raft.h"
#include "RaftRPCClient.h"
#include "raftRPC.pb.h"
#include <memory>
#include <mutex>
#include <string>
#include<thread>
void RaftNode::start() {
    {
        std::lock_guard<std::mutex> lck1(mu_state_);
        //初始化节点信息
        state_ = State::Follower;
        current_term_ = 0;
        vote_count_ = 0;
        vote_for_ = -1;
        is_heartbeat_ = false;
        is_election_success_ = false;
        //构建连接
        for(const auto[id,node]:nodes_)
        {
            clis[id] = std::make_unique<RaftRPCClient>(node.address_);
        }
    }
    while(true)
    {
        switch (state_) {
            case State::Follower:
                followerRun();
            break;
            case State::Candidate:
                candidateRun();
            break;
            case State::Leader:
                leaderRun();
            break;
        }
    }
}

void RaftNode::followerRun() {

    {
        std::unique_lock<std::mutex> lck(mu_heartbeat_);
        is_heartbeat_ = false;
        //设置心跳定时器,定时为100~200ms
        auto dest = std::chrono::steady_clock::now()+std::chrono::milliseconds(rng_()%300+500);
        cond_heartbeat_.wait_until(lck,dest,[this]()->bool{
            return is_heartbeat_ == true;
        });
        
        //过期且未收到心跳包
        if(!is_heartbeat_)
        {
            state_ = State::Candidate;
            return;
        }
    }
}
void RaftNode::candidateRun() {

    //修改任期
    current_term_ ++;
    printf("term=%d:参与选举\n",this->current_term_);
    //为自己投票
    vote_count_ =1;
    vote_for_ = me_;

    //广播要票请求
    broadcastRequestVote();
    //启动选举定时器,过期则降为follower
    std::unique_lock<std::mutex> lck(mu_election_success_);
    //设置选举定时器,定时为500~1000ms
    auto dest = std::chrono::steady_clock::now()+std::chrono::milliseconds( 300+rng_()%(5000-300));
    cond_election_success_.wait_until(lck,dest,[this]()->bool{
        return is_election_success_ == true;
    });
    //过期且未选举成功
    if(!is_election_success_)
    {
        printf("选举失败\n");
        state_ = State::Follower;
        return;
    }else
    {
        printf("选举成功\n");
        //选举成功
        is_election_success_ = false;
        state_ = State::Leader;
        //初始化日志状态
        logInit();
        //模拟客户端定期发送日志
        cliLike();
        return;
    }
}
void RaftNode::leaderRun() {

    //每隔100ms广播心跳包
    broadcastHeartBeat();       
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}
void RaftNode::onHeartBeat(const raft::HeartBeatRequest* request,raft::HeartBeatReply* response) {
    
    //TODO:如果有日志,则进行日志处理
    //1.判断"前一条日志"是否匹配,如果匹配,进行追加
    //2.如果不匹配,更新reply中的nextIndex,告知leader下次发送
    {
        std::lock_guard<std::mutex> lck1(mu_state_);
        if(this->current_term_ < request->term())
        {
            //如果leader任期更新,则修改当前节点任期
            this->current_term_ = request->term();
            this->vote_for_ = -1;
            this->state_ = State::Follower;
        }else if(this->current_term_> request->term())
        {
            //如果leader任期更旧,不做处理
            response->set_term(this->current_term_);
            response->set_success(false);
            return;
        }
        response->set_term(this->current_term_);
        //心跳成功
        {
            std::lock_guard<std::mutex> lck2(mu_heartbeat_);
            this->is_heartbeat_ = true;
        }
        //如果没有日志
        if(request->entries_size() == 0)
        {   
            response->set_next_index(0);
        }else{
            //如果有日志,要进行检查,并附加日志
            //从节点落后
            if(request->prev_index() >this->getLastIndex())
            {
                response->set_success(false);
                response->set_next_index(this->getLastIndex()+1);
            }else if(request->prev_index() == this->getLastIndex())
            {
                //匹配成功,附加日志
                response->set_success(true);
                auto entries = request->entries();
                if(this->log_.empty())
                {
                    this->log_.emplace_back(Entry{});
                }
                for(const auto e:entries)
                {
                    this->log_.emplace_back(Entry{e.index(),e.term(),e.cmd()});
                }
                response->set_next_index(this->getLastIndex()+1);
                this->commited_index_ = this->getLastIndex();
                std::cout << "追加成功,当前日志为:" << std::endl;
                for(const auto s : log_)
                {
                    std::cout << "(" << s.index << ',' << s.term <<')' << s.cmd << std::endl;
                }
            }
        
        }
    }
    cond_heartbeat_.notify_one();
}
//广播要票请求
void RaftNode::broadcastRequestVote() {

    for(const auto&[id,node]:nodes_)
    {
        std::thread th([this,id = id](){
            sendRequestVote(id);
        });
        th.detach();
    }
}
void RaftNode::sendRequestVote(int id)
{
    raft::VoteRequest request;
    raft::VoteReply response;
    std::unique_lock<std::mutex> lck(mu_state_);
    if(state_ != State::Candidate)
    {
        return;
    }
    request.set_candidate_id(this->me_);
    request.set_term(this->current_term_);
    lck.unlock();
    bool res = clis[id]->SendRequestVote(request,&response);
    if(!res)    return;
    // printf("Vote:id=%d,term=%d\n",id,request.term());
    // printf("VoteRes:granted=%d,term=%d\n",response->vote_granted(),response->term());
    lck.lock();
    if(state_ != State::Candidate)
    {
        return;
    }
    if(response.term()>this->current_term_)
    {
        //更新任期
        this->current_term_ = response.term();
        vote_for_ = -1;
        state_ = State::Follower;
        return;
    }
    if(response.vote_granted() && this->state_ == State::Candidate && response.term() == request.term())
    {
        this->vote_count_++;
        if(this->vote_count_>=(this->nodes_.size()+1)/2+1)
        {
            {
                std::lock_guard<std::mutex>lck(mu_election_success_);
                this->is_election_success_=true;
            }
            cond_election_success_.notify_one();
        }
    }
}

//广播心跳包
void RaftNode::broadcastHeartBeat() {
    //TODO:对每个结点判断是否有可发日志,并封装日志信息
    for(const auto&[id,_] : nodes_)
    {
        std::thread th([this,id = id](){
            sendHeartBeat(id);
        });
        th.detach();
    }
}
void RaftNode::sendHeartBeat(int id)
{
    raft::HeartBeatRequest request;
    raft::HeartBeatReply response;
    std::unique_lock<std::mutex>lck1(mu_state_);
    //封装请求包
    if(this->state_ != State::Leader)
    {
        return;
    }
    request.set_leader_id(me_);
    request.set_term(current_term_);
    request.set_commited_index(this->commited_index_);
    int prevIndex = this->next_indexs_[id]-1;
    //还有记录未发送,打包发送
    if(getLastIndex() > prevIndex)
    {
        request.set_prev_index(prevIndex);
        request.set_prev_term(this->log_[prevIndex].term);
        for(auto it = this->log_.begin()+prevIndex+1;it!=this->log_.end();it++)
        {
            auto e = request.add_entries();
            e->set_index(it->index);
            e->set_term(it->term);
            e->set_cmd(it->cmd);
        }
    }

    lck1.unlock();
    //数据传输
    bool res = clis[id]->SendHeartBeat(request,&response);
    if(!res)    return;
    lck1.lock();

    //接收并解析响应
    if(this->state_ != State::Leader)
    {
        return;
    }
    //如果自己任期更旧,降至follower状态
    if(this->current_term_ < response.term())
    {
        this->current_term_ = response.term();
        vote_for_ = -1;
        state_ = State::Follower;
        return;
    }
    //TODO:leader根据从结点返回的nextIndex更新对follower日志状态

    if(response.next_index()>0)
    {
        this->next_indexs_[id] = response.next_index();
        this->match_indexs_[id] = response.next_index()-1;
    } 
    //TODO:超过半数提交成功,则提交当前日志
    
}
    
    

void RaftNode::onRequestVote(const raft::VoteRequest* request,raft::VoteReply* response) {
    //降至Follower并投票
    if(request->term()>this->current_term_)
    {
        this->current_term_ = request->term();
        this->vote_for_ = request->candidate_id();
        response->set_term(this->current_term_);
        response->set_vote_granted(true);
        printf("hhhhterm=%d:投票给%d\n",this->current_term_,this->vote_for_);
        if(this->state_ == State::Candidate)
        {
            {
            std::lock_guard<std::mutex>lck(mu_election_success_);
            this->is_election_success_ = false;
            }
            this->cond_election_success_.notify_one();
        }
        this->state_ = State::Follower;
        return;
    }
    //如果候选者过期,拒绝投票
    if(request->term()<this->current_term_)
    {
        response->set_term(this->current_term_);
        response->set_vote_granted(false);
        return;
    }

    //如果没投票,
    if(this->vote_for_ ==-1)
    {
        this->current_term_ = request->term();
        this->vote_for_ = request->candidate_id();
        response->set_term(this->current_term_);
        response->set_vote_granted(true);
        printf("term=%d:投票给%d\n",this->current_term_,this->vote_for_);

        return;
    }
    response->set_term(this->current_term_);
    response->set_vote_granted(false);
    return;

}
void RaftNode::logInit() {
    commited_index_ = 0;
    last_applied_ = 0;
    for(const auto&[id,v]:nodes_)
    {
        next_indexs_[id] = getLastIndex()+1;
        match_indexs_[id] = 0;
    }
}
void RaftNode::cliLike() {
    std::thread th([this](){
        int i = 0;
        this->log_.emplace_back(Entry{});
        while(this->state_ == State::Leader)
        {
            i++;
            this->log_.emplace_back(Entry{i,this->current_term_,"test_"+std::to_string(i)});
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    });
    th.detach();
}