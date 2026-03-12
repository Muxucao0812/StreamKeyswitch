#pragma once
#include "model/card.h"
#include "model/link.h"
#include <vector>



struct ResourcePool {
    uint32_t pool_id = 0;
    std::vector<CardId> cards;
};


struct SystemState {
    Time now = 0;
    std::vector<CardState> cards;
    std::vector<LinkState> links;
    std::vector<ResourcePool> pools;
};
