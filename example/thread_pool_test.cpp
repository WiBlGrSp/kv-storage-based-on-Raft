#include"thread_pool.h"
#include <chrono>
#include<iostream>
#include <thread>
void ExampleTask() {
    while(true)
    {
        std::cout << "Task executed by thread " << std::this_thread::get_id() << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
int main()
{
    ThreadPool thp(3);
    thp.AddTask(ExampleTask);
    thp.AddTask(ExampleTask);
    thp.AddTask(ExampleTask);
    thp.AddTask(ExampleTask);

    return 0;
}