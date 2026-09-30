# kv-storage-based-on-Raft
## 1.项目简介
  本项目是基于raft共识算法实现的分布式高可用kv存储系统,基于C++ gRPC框架;支持分布式选举,日志复制,状态机应用,提供了客户端程序
### 技术栈
本项目采用 C++17 开发，基于 Raft 共识算法实现分布式 KV 存储原型。节点之间通过 gRPC 完成选举、心跳和日志复制，使用 Protobuf 定义通信协议，并通过本地文件持久化 Raft 状态与日志。

| 技术 | 用途 |
|---|---|
| C++17 | 项目主要开发语言 |
| CMake | 跨平台构建与模块依赖管理 |
| Ninja | 推荐的 CMake 构建生成器 |
| gRPC | 节点间 RPC、客户端请求通信 |
| Protocol Buffers | RPC 服务与消息序列化 |
| Raft | Leader 选举、日志复制、提交与应用 |
| C++ STL | 容器、线程、互斥锁、条件变量、Future |
| Thread Pool | 通用线程池模块 |
| 文件持久化 | 保存 `currentTerm`、`votedFor` 和 Raft 日志 |
| KV 状态机 | 基于内存 `std::map` 实现键值存储 |
| AddressSanitizer | Debug 构建下的内存错误检测 |
| clang-tidy | C++ 命名和静态规范检查 |

### 核心模块

- `raft_core`：Raft 节点状态机、选举、心跳、日志复制和持久化
- `raft_proto`：Raft 节点间通信协议
- `server_proto`：客户端与服务端通信协议
- `kv_store`：内存 KV 状态机
- `storage`：Raft 与 KV 状态机之间的适配层
- `server`：节点启动、gRPC 服务注册和客户端请求处理
- `client`：客户端连接、Leader 发现和命令交互
- `common`：线程池等通用基础设施

### 技术特点

- 支持 Leader 选举、心跳维持和日志复制
- 使用 gRPC 实现节点及客户端通信
- 使用 Protobuf 生成服务端和客户端代码
- 使用独立文件保存 Raft 状态和日志
- 使用 KV 状态机支持 `put`、`get`、`del` 操作
- 使用 AddressSanitizer 辅助检测内存问题
- 使用C++11提供的同步互斥机制确保多线程安全
  
## 2.运行方法
### 运行环境
* 操作系统:本项目示例环境为Ubuntu 24.04
* C++标准 :C++17
* 构建工具: CMake3.16及以上
* 依赖库
  * gRPC C++
  * Protocol Buffers
  * Threads 
### 编译
* 克隆项目到本地
* 编译:本项目提供了`.build.sh`进行编译
```
cd yourproject
./build.sh
```
### 运行
本项目提供了3个节点+客户端的示例
* 运行3个分布式节点
  * 在根目录下创建data目录
  * 在3个终端分别运行
    ```
    ./run/1start.sh
    ./run/2start.sh
    ./run/3start.sh
    ```
运行成功截图:
<img width="1582" height="432" alt="image" src="https://github.com/user-attachments/assets/3e29facc-074c-44ef-a02a-47aaa46971fa" />
* 运行客户端程序
  再新开一个终端,运行
  ```
  ./bin/client_test
  ```
  启动后允许输入以下指令
  ```
  put key value
  get key
  del key
  ```

## 3.项目目录树
```text
Raft/
├── CMakeLists.txt
├── build.sh  //构建脚本
├── bin/      //构建后,二进制文件目录
├── build/    //构建目录
├── lib/      //静态库目录
├── example/  //示例程序目录
│   ├── CMakeLists.txt
│   ├── main.cpp
│   ├── client_test.cpp  //客户端测试程序
│   ├── server_test.cpp  //服务端节点测试程序
│   └── thread_pool_test.cpp
├── run/      //启动脚本
│   ├── 1start.sh     //启动1号节点
│   ├── 2start.sh     //启动2号节点
│   └── 3start.sh     //启动3号节点
└── src/      //源码目录
    ├── CMakeLists.txt
    ├── common/  //工具库--线程池
    │   ├── CMakeLists.txt
    │   ├── include/
    │   │   └── thread_pool.h
    │   └── thread_pool.cpp
    ├── raft_proto/  //raft_core的proto目录--定义了raft_core所需rpc服务
    │   ├── CMakeLists.txt
    │   ├── raft_rpc.proto
    │   ├── raft_rpc.pb.h
    │   ├── raft_rpc.pb.cc
    │   ├── raft_rpc.grpc.pb.h
    │   └── raft_rpc.grpc.pb.cc
    ├── server_proto/  //服务器的proto目录--定义了服务器所需rpc服务
    │   ├── CMakeLists.txt
    │   ├── server_rpc.proto
    │   ├── server_rpc.pb.h
    │   ├── server_rpc.pb.cc
    │   ├── server_rpc.grpc.pb.h
    │   └── server_rpc.grpc.pb.cc
    ├── raft_core/    //raft核心
    │   ├── CMakeLists.txt
    │   ├── include/
    │   │   ├── raft_node.h  //raft节点定义
    │   │   ├── raft_rpc.h   //raft RPC请求定义  
    │   │   ├── raft_rpc_client.h  //raft RPC响应定义
    │   │   └── persister.h  //持久化节点
    │   ├── raft_node.cpp
    │   ├── raft_rpc.cpp
    │   ├── raft_rpc_client.cpp
    │   └── persister.cpp
    ├── kv_store/    //kv存储模块
    │   ├── CMakeLists.txt
    │   ├── include/
    │   │   └── kv_store.h
    │   └── kv_store.cpp
    ├── storage/    //统一存储接口--封装了kv_store和raft_core
    │   ├── CMakeLists.txt
    │   ├── include/
    │   │   ├── storage.h
    │   │   └── raft_storage.h
    │   └── raft_storage.cpp
    ├── server/    //服务器模块--接收客户端请求,调用下层接口
    │   ├── CMakeLists.txt
    │   ├── include/
    │   │   ├── server.h
    │   │   └── server_rpc.h
    │   ├── server.cpp
    │   └── server_rpc.cpp
    └── client/    //客户端模块--接收用户输入
        ├── CMakeLists.txt
        ├── include/
        │   └── client.h
        └── client.cpp
```
