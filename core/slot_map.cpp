#include <unistd.h>
#include <algorithm>

#include "slot_map.hpp"
#include "server.hpp"
#include "fdutil.hpp"
#include "proxy.hpp"
#include "exceptions.hpp"
#include "utils/random.hpp"
#include "utils/logging.hpp"
#include "utils/string.h"

using namespace cerb;

namespace {

    void map_slot_to(std::map<slot, util::Address>& map,
                     std::string const& slot_rep, util::Address const& addr)
    {
        slot s = util::atoi(slot_rep.data()) + 1;
        LOG(DEBUG) << "Map slot " << s << " to " << addr.host << ':' << addr.port;
        map.insert(std::make_pair(s, addr));
    }

    void set_slot_to(std::map<slot, util::Address>& map, std::string address,
                     std::vector<std::string>::iterator slot_range_begin,
                     std::vector<std::string>::iterator slot_range_end)
    {
        util::Address addr(util::Address::from_host_port(address));
        std::for_each(
            slot_range_begin, slot_range_end,
            [&](std::string const& slot_range)
            {
                if (slot_range[0] == '[') {
                    return;
                }
                std::vector<std::string> begin_end(
                    util::split_str(slot_range, "-", true));
                if (begin_end.empty()) {
                    return;
                }
                if (begin_end.size() == 1) {
                    return map_slot_to(map, begin_end[0], addr);
                }
                return map_slot_to(map, begin_end[1], addr);
            });
    }

    std::string const CLUSTER_NODES_CMD("*2\r\n$7\r\ncluster\r\n$5\r\nnodes\r\n");

}

SlotMap::SlotMap()
{
    std::fill(this->begin(), this->end(), nullptr);
}

bool SlotMap::all_covered() const
{
    return std::none_of(this->begin(), this->end(), [](Server* s) { return s == nullptr; });
}

std::set<Server*> SlotMap::replace_map(std::map<slot, util::Address> map, Proxy* proxy)
{
    std::set<Server*> removed;
    std::set<Server*> new_mapped;
    slot last_slot = 0;
    for (auto& item: map) {
        for (; last_slot < item.first; ++last_slot) {
            removed.insert(_servers[last_slot]);
            this->_servers[last_slot] = Server::get_server(item.second, proxy);
            new_mapped.insert(_servers[last_slot]);
        }
    }
    std::set<Server*> r;
    std::set_difference(
        removed.begin(), removed.end(), new_mapped.begin(), new_mapped.end(),
        std::inserter(r, r.end()));
    r.erase(nullptr);
    return std::move(r);
}

Server* SlotMap::random_addr() const
{
    return this->_servers[util::randint(0, CLUSTER_SLOT_COUNT)];
}

std::map<slot, util::Address> cerb::parse_slot_map(std::string const& nodes_info)
{
    std::vector<std::string> lines(util::split_str(nodes_info, "\n", true));
    std::map<slot, util::Address> slot_map;
    std::for_each(lines.begin(), lines.end(),
                  [&](std::string const& line) {
                      std::vector<std::string> line_cont(
                          util::split_str(line, " ", true));
                      if (line_cont.size() < 9) {
                          return;
                      }
                      if (line_cont[2].find("fail") != std::string::npos) {
                          return;
                      }
                      set_slot_to(slot_map, line_cont[1],
                                  line_cont.begin() + 8, line_cont.end());
                  });
    return std::move(slot_map);
}

void cerb::write_slot_map_cmd_to(int fd)
{
    if (-1 == ::write(fd, CLUSTER_NODES_CMD.c_str(), CLUSTER_NODES_CMD.size())) {
        throw IOError("Fetch cluster nodes info", errno);
    }
}
