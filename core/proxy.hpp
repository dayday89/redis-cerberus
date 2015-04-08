#ifndef __CERBERUS_PROXY_HPP__
#define __CERBERUS_PROXY_HPP__

#include <vector>

#include "utils/pointer.h"
#include "command.hpp"
#include "slot_map.hpp"
#include "connection.hpp"

namespace cerb {

    class Proxy;
    class Server;

    class Acceptor
        : public Connection
    {
        Proxy* const _proxy;
    public:
        Acceptor(int fd, Proxy* p)
            : Connection(fd)
            , _proxy(p)
        {}

        void triggered(int events);
        void on_error();
    };

    class SlotsMapUpdater
        : public Connection
    {
        Proxy* _proxy;
        std::map<slot, util::Address> _updated_map;
        Buffer _rsp;

        void _send_cmd();
        void _recv_rsp();
        void _await_data();
    public:
        SlotsMapUpdater(util::Address const& addr, Proxy* p);

        void triggered(int events);
        void on_error();

        bool success() const
        {
            return !_updated_map.empty();
        }

        std::map<slot, util::Address> deliver_map()
        {
            return std::move(_updated_map);
        }
    };

    class Proxy {
        int _clients_count;

        SlotMap _server_map;
        std::vector<util::sptr<SlotsMapUpdater>> _slot_updaters;
        std::vector<util::sptr<SlotsMapUpdater>> _finished_slot_updaters;
        int _active_slot_updaters_count;
        std::vector<util::sref<Command>> _retrying_commands;
        Interval _total_cmd_elapse;
        long _total_cmd;
        bool _server_closed;

        bool _should_update_slot_map() const;
        void _retrieve_slot_map();
        void _set_slot_map(std::map<slot, util::Address> map);
        void _update_slot_map();
        void _loop();
    public:
        int epfd;

        explicit Proxy(util::Address const& remote);
        ~Proxy();

        Proxy(Proxy const&) = delete;

        int clients_count() const
        {
            return _clients_count;
        }

        long total_cmd() const
        {
            return _total_cmd;
        }

        Interval total_cmd_elapse() const
        {
            return _total_cmd_elapse;
        }

        Server const* random_addr() const
        {
            return _server_map.random_addr();
        }

        Server* get_server_by_slot(slot key_slot);
        void notify_slot_map_updated();
        void server_closed();
        void retry_move_ask_command_later(util::sref<Command> cmd);
        void run(int listen_port);
        void accept_from(int listen_fd);
        void pop_client(Client* cli);
        void stat_proccessed(Interval cmd_elapse);
    };

}

#endif /* __CERBERUS_PROXY_HPP__ */
