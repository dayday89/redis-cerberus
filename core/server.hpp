#ifndef __CERBERUS_SERVER_HPP__
#define __CERBERUS_SERVER_HPP__

#include "buffer.hpp"
#include "connection.hpp"
#include "utils/pointer.h"
#include "utils/address.hpp"

namespace cerb {

    class Proxy;
    class Client;
    class Command;

    class Server
        : public ProxyConnection
    {
        Proxy* _proxy;
        Buffer _buffer;

        std::vector<util::sref<Command>> _commands;
        std::vector<util::sref<Command>> _ready_commands;

        void _send_to();
        void _recv_from();
        void _reconnect(util::Address const& addr, Proxy* p);

        Server()
            : ProxyConnection(-1)
            , _proxy(nullptr)
            , addr("", 0)
        {}

        ~Server() = default;

        static Server* _alloc_server(util::Address const& addr, Proxy* p);
    public:
        util::Address addr;

        static Server* get_server(util::Address addr, Proxy* p);
        static void close_server(Server* server);
        static std::map<util::Address, Server*>::iterator addr_begin();
        static std::map<util::Address, Server*>::iterator addr_end();

        void triggered(int events);
        void event_handled(std::set<Connection*>&);

        void push_client_command(util::sref<Command> cmd);
        void pop_client(Client* cli);
        std::vector<util::sref<Command>> deliver_commands();
    };

}

#endif /* __CERBERUS_SERVER_HPP__ */
