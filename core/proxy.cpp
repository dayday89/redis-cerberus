#include <unistd.h>
#include <netdb.h>
#include <sys/epoll.h>

#include "proxy.hpp"
#include "server.hpp"
#include "client.hpp"
#include "response.hpp"
#include "exceptions.hpp"
#include "utils/string.h"
#include "utils/alg.hpp"
#include "utils/logging.hpp"

using namespace cerb;

static int const MAX_EVENTS = 1024;

void Acceptor::triggered(int)
{
    _proxy->accept_from(this->fd);
}

void Acceptor::on_error()
{
    LOG(ERROR) << "Accept error.";
}

SlotsMapUpdater::SlotsMapUpdater(util::Address const& addr, Proxy* p)
    : Connection(new_stream_socket())
    , _proxy(p)
{
    set_nonblocking(fd);
    connect_fd(addr.host, addr.port, this->fd);
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.ptr = this;
    if (epoll_ctl(_proxy->epfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        throw SystemError("epoll_ctl#add", errno);
    }
}

void SlotsMapUpdater::_await_data()
{
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = this;
    if (epoll_ctl(_proxy->epfd, EPOLL_CTL_MOD, this->fd, &ev) == -1) {
        throw SystemError("epoll_ctl#modi", errno);
    }
}

void SlotsMapUpdater::_send_cmd()
{
    write_slot_map_cmd_to(this->fd);
    this->_await_data();
}

void SlotsMapUpdater::_recv_rsp()
{
    _rsp.read(this->fd);
    LOG(DEBUG) << "+read slots from " << this->fd
               << " buffer size " << this->_rsp.size()
               << ": " << this->_rsp.to_string();
    std::vector<util::sptr<Response>> rsp(split_server_response(_rsp));
    if (rsp.size() == 0) {
        return this->_await_data();
    }
    if (rsp.size() != 1) {
        throw BadRedisMessage("Ask cluster nodes returns responses with size=" +
                              util::str(int(rsp.size())));
    }
    this->_updated_map = std::move(
        parse_slot_map(rsp[0]->dump_buffer().to_string()));
    _proxy->notify_slot_map_updated();
}

void SlotsMapUpdater::triggered(int events)
{
    if (events & EPOLLRDHUP) {
        LOG(ERROR) << "Failed to retrieve slot map from " << this->fd
                   << ". Closed because remote hung up.";
        throw ConnectionHungUp();
    }
    if (events & EPOLLIN) {
        try {
            this->_recv_rsp();
        } catch (BadRedisMessage& e) {
            LOG(FATAL) << "Receive bad message from server on update from "
                       << this->fd
                       << " because: " << e.what()
                       << " buffer length=" << this->_rsp.size()
                       << " dump buffer (before close): "
                       << this->_rsp.to_string();
            exit(1);
        }
    }
    if (events & EPOLLOUT) {
        this->_send_cmd();
    }
}

void SlotsMapUpdater::on_error()
{
    epoll_ctl(_proxy->epfd, EPOLL_CTL_DEL, this->fd, NULL);
    _proxy->notify_slot_map_updated();
}

Proxy::Proxy(util::Address const& remote)
    : _clients_count(0)
    , _server_map([this](std::string const& host, int port)
                  {
                      return new Server(host, port, this);
                  })
    , _active_slot_updaters_count(0)
    , _total_cmd_elapse(0)
    , _total_cmd(0)
    , _server_closed(false)
    , epfd(epoll_create(MAX_EVENTS))
{
    if (epfd == -1) {
        throw SystemError("epoll_create", errno);
    }
    _slot_updaters.push_back(util::mkptr(new SlotsMapUpdater(remote, this)));
    ++_active_slot_updaters_count;
}

Proxy::~Proxy()
{
    ::close(epfd);
}

void Proxy::_update_slot_map()
{
    if (_slot_updaters.end() == std::find_if(
            _slot_updaters.begin(), _slot_updaters.end(),
            [&](util::sptr<SlotsMapUpdater>& updater)
            {
                if (!updater->success()) {
                    LOG(DEBUG) << "Discard a failed node";
                    return false;
                }
                std::map<slot, util::Address> m(updater->deliver_map());
                for (auto i = m.begin(); i != m.end(); ++i) {
                    if (i->second.host.empty()) {
                        LOG(DEBUG) << "Discard result because address is empty string "
                                   << ':' << i->second.port;
                        return false;
                    }
                }
                _set_slot_map(std::move(m));
                return true;
            }))
    {
        throw BadClusterStatus("Fail to update slot mapping");
    }
    _finished_slot_updaters = std::move(_slot_updaters);
    LOG(INFO) << "Slot map updated";
}

void Proxy::_set_slot_map(std::map<slot, util::Address> map)
{
    auto s(_server_map.set_map(std::move(map)));
    std::for_each(s.begin(), s.end(),
                  [&](Server* s)
                  {
                      std::vector<util::sref<Command>> c(s->deliver_commands());
                      _retrying_commands.insert(_retrying_commands.end(),
                                                c.begin(), c.end());
                      delete s;
                  });

    if (!_server_map.all_covered()) {
        LOG(ERROR) << "Map not covered all slots";
        return _retrieve_slot_map();
    }

    _server_closed = false;
    if (_retrying_commands.empty()) {
        return;
    }

    LOG(DEBUG) << "Retry MOVED or ASK: " << _retrying_commands.size();
    std::set<Server*> svrs;
    std::vector<util::sref<Command>> retrying(std::move(_retrying_commands));
    std::for_each(retrying.begin(), retrying.end(),
                  [&](util::sref<Command> cmd)
                  {
                      Server* s = cmd->select_server(this);
                      if (s == nullptr) {
                          return;
                      }
                      svrs.insert(s);
                  });

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    std::for_each(svrs.begin(), svrs.end(),
                  [&](Server* svr)
                  {
                      ev.data.ptr = svr;
                      if (epoll_ctl(this->epfd, EPOLL_CTL_MOD,
                                    svr->fd, &ev) == -1)
                      {
                          throw SystemError(
                              "epoll_ctl+modio Proxy::_set_slot_map", errno);
                      }
                  });
}

void Proxy::_retrieve_slot_map()
{
    _server_map.iterate_addr(
        [&](util::Address const& addr)
        {
            try {
                _slot_updaters.push_back(
                    util::mkptr(new SlotsMapUpdater(addr, this)));
                ++_active_slot_updaters_count;
            } catch (ConnectionRefused& e) {
                LOG(INFO) << e.what();
                LOG(INFO) << "Disconnected: " << addr.host << ':' << addr.port;
            } catch (UnknownHost& e) {
                LOG(ERROR) << e.what();
            }
        });
    if (_slot_updaters.empty()) {
        throw BadClusterStatus("No nodes could be reached");
    }
}

void Proxy::notify_slot_map_updated()
{
    if (--_active_slot_updaters_count == 0) {
        _update_slot_map();
    }
}

void Proxy::server_closed()
{
    _server_closed = true;
}

bool Proxy::_should_update_slot_map() const
{
    return _slot_updaters.empty() &&
        (!_retrying_commands.empty() || _server_closed);
}

void Proxy::retry_move_ask_command_later(util::sref<Command> cmd)
{
    _retrying_commands.push_back(cmd);
    LOG(DEBUG) << "A MOVED or ASK added for later retry - " << _retrying_commands.size();
}

void Proxy::run(int listen_port)
{
    cerb::Acceptor acc(new_stream_socket(), this);
    set_nonblocking(acc.fd);
    bind_to(acc.fd, listen_port);

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = &acc;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, acc.fd, &ev) == -1) {
        throw SystemError("epoll_ctl*listen", errno);
    }

    while (true) {
        this->_loop();
    }
}

void Proxy::_loop()
{
    struct epoll_event events[MAX_EVENTS];
    int nfds = epoll_wait(this->epfd, events, MAX_EVENTS, -1);
    if (nfds == -1) {
        if (errno == EINTR) {
            return;
        }
        throw SystemError("epoll_wait", errno);
    }

    std::set<Connection*> active_conns;
    std::set<Connection*> closed_conns;
    LOG(DEBUG) << "*epoll wait: " << nfds;
    for (int i = 0; i < nfds; ++i) {
        Connection* conn = static_cast<Connection*>(events[i].data.ptr);
        LOG(DEBUG) << "*epoll process " << conn->fd;
        if (closed_conns.find(conn) != closed_conns.end()) {
            continue;
        }
        active_conns.insert(conn);
        try {
            conn->triggered(events[i].events);
        } catch (IOErrorBase& e) {
            LOG(ERROR) << "IOError: " << e.what() << " :: "
                       << "Close connection to " << conn->fd << " in " << conn;
            conn->on_error();
            closed_conns.insert(conn);
        }
    }
    LOG(DEBUG) << "*epoll done";
    std::for_each(active_conns.begin(), active_conns.end(),
                  [&](Connection* c)
                  {
                      c->event_handled(active_conns);
                  });
    _finished_slot_updaters.clear();
    if (this->_should_update_slot_map()) {
        LOG(DEBUG) << "Should update slot map";
        this->_retrieve_slot_map();
    }
}

Server* Proxy::get_server_by_slot(slot key_slot)
{
    Server* s = _server_map.get_by_slot(key_slot);
    return (s == nullptr || s->fd == -1) ? nullptr : s;
}

void Proxy::accept_from(int listen_fd)
{
    int cfd;
    struct sockaddr_in remote;
    socklen_t addrlen = sizeof remote;
    while ((cfd = accept(listen_fd, reinterpret_cast<struct sockaddr*>(&remote),
                         &addrlen)) > 0)
    {
        LOG(DEBUG) << "*accept " << cfd;
        set_nonblocking(cfd);
        set_tcpnodelay(cfd);
        Connection* c = new Client(cfd, this);
        ++this->_clients_count;
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        ev.data.ptr = c;
        if (epoll_ctl(this->epfd, EPOLL_CTL_ADD, cfd, &ev) == -1) {
            throw SystemError("epoll_ctl-add", errno);
        }
    }
    if (cfd == -1) {
        if (errno != EAGAIN && errno != ECONNABORTED
            && errno != EPROTO && errno != EINTR)
        {
            throw SocketAcceptError(errno);
        }
    }
}

void Proxy::pop_client(Client* cli)
{
    util::erase_if(
        this->_retrying_commands,
        [&](util::sref<Command> cmd)
        {
            return cmd->group->client.is(cli);
        });
    --this->_clients_count;
}

void Proxy::stat_proccessed(Interval cmd_elapse)
{
    _total_cmd_elapse += cmd_elapse;
    ++_total_cmd;
}
