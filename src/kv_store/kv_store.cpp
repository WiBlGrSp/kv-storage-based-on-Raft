#include"kv_store.h"
#include <mutex>
bool KVStore::Get(const std::string&key,std::string&value)
{
    std::lock_guard<std::mutex> lo(mu_);
    if(auto it = kv_map_.find(key);it!=kv_map_.end())
    {
        value =  kv_map_[key];
        return true;
    }
    return false;
    
}
bool KVStore::Put(const std::string&key,const std::string&value)
{
    std::lock_guard<std::mutex> lo(mu_);
    kv_map_[key] = value;
    printf("成功写入:%s:%s\n",key.c_str(),value.c_str());
    return true;
}
bool KVStore::Del(const std::string&key)
{
    std::lock_guard<std::mutex> lo(mu_);
    if(auto it = kv_map_.find(key);it!=kv_map_.end())   
    {
        kv_map_.erase(key);
        printf("成功删除:%s\n",key.c_str());
        return true;
    }
    else {
        return false;
    }
}

void KVStore::CreateSnapshot(std::string&snapshot) {
    std::lock_guard<std::mutex> lo(mu_);
    for(const auto&[key,value] : kv_map_)
    {
        snapshot.append("P:"+key+ ":" + value + "\n");
    };
}
KVStore::KVStore() {
}
KVStore::~KVStore() {
}