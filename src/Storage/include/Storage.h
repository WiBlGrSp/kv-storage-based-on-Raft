#pragma once
#ifndef STORAGE_H
#define STORAGE_H
#include<string>
//定义存储服务接口
class Storage{
    public:
    //写入键值对,value=''表示删除键
    //return : 成功写入返回true,写入失败返回false
    virtual bool Put(const std::string&key,const std::string &value)=0;
    //获取键对应的值,value=''表示该键不存在
    //return : 成功获取返回true,获取失败返回false
    virtual bool Get(const std::string&key,std::string&value)=0;
    virtual ~Storage()=default;
};

#endif //!STORAGE_H