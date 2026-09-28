#include"server.h"
int main(int argc,const char*argv[])
{
    printf("program start\n");
    int me;
    std::string address;
    std::map<int,std::string> nodes;
    if(argc<2)
    {
        printf("too little arguments\n");
        return -1;
    }else{
        for(int i =1;i<argc;i++)
        {
            std::string buf(argv[i]);
            auto it = buf.find('=');
            auto it2 = buf.find('@');
            int id = stoi(buf.substr(it+1,it2-it-1));
            std::string addr = buf.substr(it2+1);
            if(i == 1)
            {
                me = id;
                address = addr;
            }else{
                nodes.emplace(id,addr);
            }
        }
    }
    //初始化服务器
    Server server(me,address,nodes);
    server.Start();

    return 0;
}
