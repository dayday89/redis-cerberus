#ifndef __CERBERUS_SLOT_MAP_HPP__
#define __CERBERUS_SLOT_MAP_HPP__

#include <map>
#include <set>

#include "common.hpp"
#include "utils/address.hpp"

namespace cerb {

    int const CLUSTER_SLOT_COUNT = 16384;

    class Server;
    class Proxy;

    class SlotMap {
        Server* _servers[CLUSTER_SLOT_COUNT];
    public:
        SlotMap();
        SlotMap(SlotMap const&) = delete;

        Server** begin()
        {
            return _servers;
        }

        Server** end()
        {
            return _servers + CLUSTER_SLOT_COUNT;
        }

        Server* const* begin() const
        {
            return _servers;
        }

        Server* const* end() const
        {
            return _servers + CLUSTER_SLOT_COUNT;
        }

        Server* get_by_slot(slot s)
        {
            return _servers[s];
        }

        bool all_covered() const;
        std::set<Server*> replace_map(std::map<slot, util::Address> map, Proxy* proxy);
        Server* random_addr() const;
    };

    std::map<slot, util::Address> parse_slot_map(std::string const& nodes_info);
    void write_slot_map_cmd_to(int fd);

}

#endif /* __CERBERUS_SLOT_MAP_HPP__ */
