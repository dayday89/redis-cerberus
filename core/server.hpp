#ifndef __CERBERUS_SERVER_HPP__
#define __CERBERUS_SERVER_HPP__

#include "utils/pointer.h"
#include "connection.hpp"

namespace cerb {

    class Client;
    class Command;

    class Server
        : public ProxyConnection
    {
        Proxy* const _proxy;
        Buffer _buffer;

        std::vector<util::sref<Command>> _commands;
        std::vector<util::sref<Command>> _ready_commands;

        void _send_to();
        void _recv_from();
    public:
        Server(std::string const& host, int port, Proxy* p);

        void triggered(int events);
        void event_handled(std::set<Connection*>&);

        void push_client_command(util::sref<Command> cmd);
        void pop_client(Client* cli);
        std::vector<util::sref<Command>> deliver_commands();
    };

}

#endif /* __CERBERUS_SERVER_HPP__ */
