#include"raft.h"
#include <chrono>
#include <cstdlib>
#include "KVStore.h"
#include "RaftRPCClient.h"
#include "raftRPC.pb.h"
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include<thread>
#include <tuple>
#include <utility>
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
        commited_index_ = 0;
        last_applied_ = 0;
        //加载持久化信息
        loadState();
        loadLog();
        //构建连接
        for(const auto[id,node]:peers_)
        {
            clis[id] = std::make_unique<RaftRPCClient>(node.address_);
        }
        //状态机应用
        applyLoop();
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

    std::unique_lock<std::mutex> ulck(mu_state_);
    //修改任期
    current_term_ ++;
    //为自己投票
    vote_count_ =1;
    vote_for_ = me_;
    saveState();
    printf("term=%d:参与选举\n",this->current_term_);


    ulck.unlock();
    //广播要票请求
    broadcastRequestVote();
    //启动选举定时器,过期则降为follower
    std::unique_lock<std::mutex> lck(mu_election_success_);
    is_election_success_ = false;
    //设置选举定时器,定时为500~1000ms
    auto dest = std::chrono::steady_clock::now()+std::chrono::milliseconds( 300+rng_()%(5000-300));
    cond_election_success_.wait_until(lck,dest,[this]()->bool{
        return is_election_success_ == true || this->state_ !=State::Candidate;
    });
    ulck.lock();
    //过期且未选举成功
    if(this->state_ !=State::Candidate)
    {
        return;
    }
    if(!is_election_success_)
    {
        printf("选举失败\n");
        state_ = State::Follower;
        return;
    }else
    {
        printf("选举成功\n");
        //选举成功
        state_ = State::Leader;
        //初始化日志状态
        logInit();
        // //模拟客户端定期发送日志
        // cliLike();
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

    std::lock_guard<std::mutex> lck1(mu_state_);
    if(this->current_term_ < request->term())
    {
        //如果leader任期更新,则修改当前节点任期
        this->current_term_ = request->term();
        this->vote_for_ = -1;
        this->state_ = State::Follower;
        saveState();
    }else 
    if(this->current_term_> request->term())
    {
        //如果leader任期更旧,不做处理
        response->set_term(this->current_term_);
        response->set_success(false);
        return;
    }
    response->set_term(this->current_term_);
    //心跳成功
    this->state_ = State::Follower;
    resetHeartBeatTimer();

    //处理日志组
    
    //没有日志
    if(request->entries_size() == 0)
    {   
        response->set_success( true);

    }else
    {
        //如果有日志

        if(this->getLastIndex() < request->prev_index())
        {
            //从节点没有该日志
            response->set_success(false);
            // response->set_next_index(this->getLastIndex()+1);
        }else 
        {
            //从节点有该日志
            int prev_index = request->prev_index();
            if(this->log_[prev_index].term == request->prev_term())
            {
                //匹配成功
                //删除之后的日志
                this->log_.erase(this->log_.begin()+prev_index+1,this->log_.end());
                //追加新日志
                auto entries = request->entries();
                for(const auto e:entries)
                {
                    this->log_.emplace_back(Entry{e.index(),e.term(),e.cmd()});
                }
                // response->set_next_index(this->getLastIndex()+1);
                std::cout << "追加成功,当前日志为:" << std::endl;
                for(const auto s : log_)
                {
                    std::cout << "(" << s.index << ',' << s.term <<')' << s.cmd << std::endl;
                }
                //响应成功
                response->set_success(true);
            }else{
            //匹配失败

            //删除当前日志及之后日志
            this->log_.erase(this->log_.begin()+prev_index,this->log_.end());
            //响应失败
            response->set_success(false);
            }
        }
    
    }
    //commit更新
    // printf("commited_index=%d\n",this->commited_index_);
    // printf("request_commited_index=%d\n",request->commited_index());
    if(commited_index_ < request->commited_index())
    {
        int snapshot = commited_index_;
        commited_index_ = std::min(getLastIndex(),request->commited_index());
        if(snapshot!=commited_index_)
            commitLog(commited_index_);
    }
}
//广播要票请求
void RaftNode::broadcastRequestVote() {

    //任期快照
    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_state_);
        term_snapshot = this->current_term_;
    }
    for(const auto&[id,node]:peers_)
    {
        std::thread th([this,id = id,term_snapshot](){
            sendRequestVote(id,term_snapshot);
        });
        th.detach();
    }
}
void RaftNode::sendRequestVote(int id,const int term_snapshot)
{
    raft::VoteRequest request;
    raft::VoteReply response;
    std::unique_lock<std::mutex> lck(mu_state_);
    if(!(state_ == State::Candidate && this->current_term_ == term_snapshot))
    {
        return;
    }
    request.set_candidate_id(this->me_);
    request.set_term(this->current_term_);
    //candidate发送日志追加情况
    request.set_last_log_term(getLastTerm());
    request.set_last_log_index(getLastIndex());
    lck.unlock();
    bool res = clis[id]->SendRequestVote(request,&response);
    if(!res)    return;
    // printf("Vote:id=%d,term=%d\n",id,request.term());
    // printf("VoteRes:granted=%d,term=%d\n",response->vote_granted(),response->term());
    lck.lock();
    if(this->current_term_ < response.term())
    {
        this->current_term_ = response.term();
        vote_for_ = -1;
        state_ = State::Follower;
        saveState();
        return;
    }
    if(!(state_ == State::Candidate && this->current_term_ == term_snapshot))
    {
        return;
    }
 
    if(response.vote_granted() && this->state_ == State::Candidate && response.term() == request.term())
    {
        this->vote_count_++;
        if(this->vote_count_>=(this->peers_.size()+1)/2+1)
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
    //对每个结点判断是否有可发日志,并封装日志信息
    //任期快照
    int term_snapshot;
    {
        std::lock_guard<std::mutex> lck(mu_state_);
        term_snapshot = this->current_term_;
    }
    for(const auto&[id,_] : peers_)
    {
        std::thread th([this,id = id,term_snapshot](){
            sendHeartBeat(id,term_snapshot);
        });
        th.detach();
    }
}
void RaftNode::sendHeartBeat(int id,const int term_snapshot)
{
    raft::HeartBeatRequest request;
    raft::HeartBeatReply response;
    std::unique_lock<std::mutex>lck1(mu_state_);
    //封装请求包
    if(!(this->state_ == State::Leader && this->current_term_ ==term_snapshot))
    {
        return;
    }
    request.set_leader_id(me_);
    request.set_term(current_term_);
    request.set_commited_index(this->commited_index_);
    int prevIndex = this->next_indexs_[id]-1;
    //还有记录未发送,打包发送
    if(getLastIndex() >= this->next_indexs_[id])
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

    //如果自己任期更旧,降至follower状态
    if(this->current_term_ < response.term())
    {
        this->current_term_ = response.term();
        vote_for_ = -1;
        state_ = State::Follower;
        saveState();
        return;
    }
    if(!(this->state_ == State::Leader && this->current_term_ ==term_snapshot))
    {
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
            updateCommit();
        }
    }else
    {
        //如果心跳失败,递减next,下一次心跳重试
        next_indexs_[id] --;
    }

    //leader根据从结点返回的nextIndex更新对follower日志状态

    // if(response.next_index()>0)
    // {
    //     this->next_indexs_[id] = response.next_index();
    //     this->match_indexs_[id] = response.next_index()-1;
    // } 
    //TODO:超过半数提交成功,则提交当前日志
    
}
    
    

void RaftNode::onRequestVote(const raft::VoteRequest* request,raft::VoteReply* response) {
    std::lock_guard<std::mutex> lck(mu_state_);
    bool granted = false;   //是否投票
    //任期更旧,更新任期状态
    if(this->current_term_ < request->term())
    {
        this->current_term_ = request->term();
        this->vote_for_ = -1;
        saveState();
        if(this->state_ == State::Candidate)
        {
            {
                std::lock_guard<std::mutex>lck(mu_election_success_);
                this->is_election_success_ = false;
            }
            this->state_ = State::Follower;
            this->cond_election_success_.notify_one();
        }else
        this->state_ = State::Follower;
    }
    //如果任期一致,follower未投票,candidate至少比follower新或一致,则投票
    if(this->current_term_ == request->term() && this->vote_for_ ==-1 && newerOrEqualLogs(request->last_log_term(),request->last_log_index()))
    {
        granted = true;
    }else{
        granted = false;
    }
    //投票,更新心跳定时器
    if(granted)
    {
        this->vote_for_ = request->candidate_id();
        saveState();
        response->set_vote_granted(true);
        response->set_term(this->current_term_);
        resetHeartBeatTimer();
        printf("term=%d:投票给%d\n",this->current_term_,this->vote_for_);
    }else
    {
        response->set_vote_granted(false);
        response->set_term(this->current_term_);
    }
}
void RaftNode::logInit() {
    // commited_index_ = 0;
    // last_applied_ = 0;
    for(const auto&[id,v]:peers_)
    {
        next_indexs_[id] = getLastIndex()+1;
        match_indexs_[id] = 0;
    }
}
void RaftNode::cliLike() {
    std::thread th([this](){
        int i = this->getLastIndex();
        while(this->state_==State::Leader)
        {
            i++;
            this->log_.emplace_back(Entry{i,this->current_term_,"test_"+std::to_string(i)});
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    });
    th.detach();
}
void RaftNode::resetHeartBeatTimer() {
    {
        std::lock_guard<std::mutex> lck2(mu_heartbeat_);
        this->is_heartbeat_ = true;
    }
    cond_heartbeat_.notify_one();
}
//必须外部持有锁
bool RaftNode::saveState() {
    std::string data;
    data = std::to_string(current_term_) + ":" + std::to_string(vote_for_);
    return persis_.saveRaftState(data);
}
bool RaftNode::loadState() {
    std::string data;
    bool res = persis_.readRaftState(data);
    if(res == false)
        return false;
    auto it = data.find(":");
    this->current_term_ = std::stoi(data.substr(0,it));
    this->vote_for_ = std::stoi(data.substr(it+1));
    printf("loadState :%d:%d\n",current_term_,vote_for_);
    return true;
}
//必须外部持有锁
bool RaftNode::saveLog() {

    std::stringstream ss;
    int size = log_.size();
    for(size_t i = 1;i<=size;i++)
    {
        ss << log_[i].index << ":" << log_[i].term << ":" << log_[i].cmd << "\n";
    }
    return persis_.saveRaftLog(ss.str());
}
bool RaftNode::loadLog() {
    std::string data;
    persis_.readRaftLog(data);
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
bool RaftNode::commitLog(int index)
{
    std::stringstream ss;
    int size = log_.size();
    for(size_t i = 1;i<=index;i++)
    {
        ss << log_[i].index << ":" << log_[i].term << ":" << log_[i].cmd << "\n";
    }
    return persis_.saveRaftLog(ss.str());
}
bool RaftNode::updateCommit()
{
    //搜索最大的多数match序号res
    int left = commited_index_ +1;
    int right = getLastIndex();
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
    int res;
    if(count >= (peers_.size()+1)/2+1)
    {
        res = mid;
    }else{
        res = mid-1;
    }   
    //保证commitindex有改变,且提交日志为本任期,再进行提交
    if(res!=commited_index_ && this->log_[res].term == this->current_term_){
        //提交日志,更新commit
        if(commitLog(res))
        {
            commited_index_ = res;
        }
    }

    return true;
}
RaftNode::myFuture RaftNode::propose(const std::string&cmd)
{
    std::lock_guard<std::mutex> lck(mu_state_);
    //不是领导者直接退出
    if(this->state_ !=State::Leader)
    {
            myPromise p;
            p.set_value(false);
            return p.get_future();
    }
    //追加日志
    int index = getLastIndex()+1;
    int term = this->current_term_;
    this->log_.emplace_back(Entry{index,term,cmd});
    //将客户端请求添加到pendding,等待commit与apply成功
    auto p = std::make_shared<myPromise>();
    this->pendding_map_[std::pair<int, int>(index,term)] = p;
    return p-> get_future();
}
void RaftNode::applyLoop()
{
    std::thread th([this](){
        while(true)
        {
            {
                std::lock_guard<std::mutex> lck(mu_state_);
                if(this->state_==State::Leader && last_applied_ < commited_index_)
                {
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
                            state_machine_.put(key,value);
                            success = true;
                        }
                        else if(op == "del")
                        {
                            state_machine_.del(key);
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
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    });
    th.detach();
}
