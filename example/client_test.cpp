#include"client.h"
#include <map>

int main()
{
    std::map<int,std::string> nodes;
    nodes[1] = "127.0.0.1:50051";
    nodes[2] = "127.0.0.1:50052";
    nodes[3] = "127.0.0.1:50053";
    Client client(nodes);
    client.start();
    return 0;
}
