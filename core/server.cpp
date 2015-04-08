#include <sys/epoll.h>

#include "command.hpp"
#include "server.hpp"
#include "client.hpp"
#include "proxy.hpp"
#include "response.hpp"
#include "exceptions.hpp"
#include "utils/alg.hpp"
#include "utils/logging.hpp"

using namespace cerb;

Server::Server(std::string const& host, int port, Proxy* p)
    : ProxyConnection(new_stream_socket())
    , _proxy(p)
{
    set_nonblocking(fd);
    connect_fd(host, port, this->fd);

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.ptr = this;
    if (epoll_ctl(_proxy->epfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        throw SystemError("epoll_ctl+add", errno);
    }
}

void Server::triggered(int events)
{
    if (events & EPOLLRDHUP) {
        return this->close();
    }
    if (events & EPOLLIN) {
        try {
            this->_recv_from();
        } catch (BadRedisMessage& e) {
            LOG(FATAL) << "Receive bad message from server " << this->fd
                       << " because: " << e.what()
                       << " dump buffer (before close): "
                       << this->_buffer.to_string();
            exit(1);
        }
    }
    if (events & EPOLLOUT) {
        this->_send_to();
    }
}

void Server::event_handled(std::set<Connection*>&)
{
    if (this->closed()) {
        LOG(ERROR) << "Server closed connection " << this->fd
                   << ". Notify proxy to update slot map";
        _proxy->server_closed();
    }
}

void Server::_send_to()
{
    if (this->_commands.empty()) {
        return;
    }
    if (!this->_ready_commands.empty()) {
        LOG(DEBUG) << "+busy";
        return;
    }

    std::vector<util::sref<Buffer>> buffer_arr;
    this->_ready_commands = std::move(this->_commands);
    buffer_arr.reserve(this->_ready_commands.size());
    for (auto const& c: this->_ready_commands) {
        buffer_arr.push_back(util::mkref(c->buffer));
    }
    Buffer::writev(this->fd, buffer_arr);

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = this;
    if (epoll_ctl(_proxy->epfd, EPOLL_CTL_MOD, this->fd, &ev) == -1) {
        throw SystemError("epoll_ctl+modi", errno);
    }
}

void Server::_recv_from()
{
    int n = this->_buffer.read(this->fd);
    if (n == 0) {
        LOG(INFO) << "Server hang up: " << this->fd;
        throw ConnectionHungUp();
    }
    LOG(DEBUG) << "+read from " << this->fd
               << " buffer size " << this->_buffer.size()
               << ": " << this->_buffer.to_string();
    auto responses(split_server_response(this->_buffer));
    if (responses.size() > this->_ready_commands.size()) {
        LOG(ERROR) << "+Error on split, expected size: " << this->_ready_commands.size()
                   << " actual: " << responses.size() << " dump buffer:";
        std::for_each(responses.begin(), responses.end(),
                      [](util::sptr<Response> const& rsp)
                      {
                          LOG(ERROR) << "::: " << rsp->dump_buffer().to_string();
                      });
        LOG(ERROR) << "Rest buffer: " << this->_buffer.to_string();
        LOG(FATAL) << "Exit";
        exit(1);
    }
    LOG(DEBUG) << "+responses size: " << responses.size();
    LOG(DEBUG) << "+rest buffer: " << this->_buffer.size() << ": " << this->_buffer.to_string();
    auto client_it = this->_ready_commands.begin();
    std::for_each(responses.begin(), responses.end(),
                  [&](util::sptr<Response>& rsp)
                  {
                      util::sref<Command> c = *client_it++;
                      if (c.not_nul()) {
                          rsp->rsp_to(c, util::mkref(*this->_proxy));
                      }
                  });
    this->_ready_commands.erase(this->_ready_commands.begin(), client_it);
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.ptr = this;
    if (epoll_ctl(_proxy->epfd, EPOLL_CTL_MOD, this->fd, &ev) == -1) {
        throw SystemError("epoll_ctl+modio Server::_recv_from", errno);
    }
}

void Server::push_client_command(util::sref<Command> cmd)
{
    _commands.push_back(cmd);
    cmd->group->client->add_peer(this);
}

void Server::pop_client(Client* cli)
{
    util::erase_if(
        this->_commands,
        [&](util::sref<Command> cmd)
        {
            return cmd->group->client.is(cli);
        });
    std::for_each(this->_ready_commands.begin(), this->_ready_commands.end(),
                  [&](util::sref<Command>& cmd)
                  {
                      if (cmd.not_nul() && cmd->group->client.is(cli)) {
                          cmd.reset();
                      }
                  });
}

std::vector<util::sref<Command>> Server::deliver_commands()
{
    util::erase_if(
        this->_ready_commands,
        [](util::sref<Command> cmd)
        {
            return cmd.nul();
        });
    _commands.insert(_commands.end(), _ready_commands.begin(),
                     _ready_commands.end());
    return std::move(_commands);
}
