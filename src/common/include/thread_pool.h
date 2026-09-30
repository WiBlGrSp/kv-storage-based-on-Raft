#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>             //用于存储多个线程
#include <thread>             //线程的创建及管理
#include <queue>              //用于存储多个任务的队列
#include <condition_variable> //条件变量头文件
#include <mutex>              //互斥锁头文件
#include <functional>         //用于使用函数类型

class ThreadPool
{
private:
    // 线程池相关的私有成员
    std::vector<std::thread> workers_;        // 存储工作的线程容器
    std::queue<std::function<void()>> tasks_; // 存储任务的队列
    std::mutex mu_;              // 互斥锁
    std::condition_variable cond_;    // 用于通知线程有新任务的条件变量
    bool stop_;                     // 线程池的标志，判断线程池是否停止

    // 启动线程池
    void StartThreadPool(size_t num_threads);

public:
    ThreadPool(int thread_pool_size); // 构造函数声明
    ~ThreadPool();                  // 析构函数声明

    void AddTask(std::function<void()> task); // 添加任务到线程池
};

#endif