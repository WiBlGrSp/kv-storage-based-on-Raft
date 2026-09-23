#pragma once
#ifndef KVSTORE_H
#define KVSTORE_H
#include<string>
#include<mutex>
#include<map>
typedef std::map<std::string,std::string>DataMap;
//顶层模块
class KVStore
{
private:
    //红黑树存储键值对
    DataMap kv_map_;
    //资源互斥锁
    std::mutex mu_;
public:
    KVStore();
    ~KVStore();
    //核心接口
    bool get(const std::string&key,std::string&value);
    bool put(const std::string&key,const std::string&value);
    bool del(const std::string&key);
    //创建快照
    void createSnapshot(std::string&snapshot);
};


#endif //!KVSTORE_H
