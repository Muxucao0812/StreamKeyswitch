#include "backend/analytical_backend.h"
#include "model/card.h"
#include "model/workload.h"
#include "scheduler/fifo_scheduler.h"
#include "sim/simulator.h"
#include <memory>

int main() {
    SystemState initial_state;
    initial_state.now = 0;
    initial_state.fabric_link.available_time = 0;
    initial_state.fabric_link.bandwidth_bytes_per_ns = 64.0;

    for (CardId card_id = 0; card_id < 16; ++card_id) {
        CardState card;
        card.card_id = card_id;
        card.memory_capacity_bytes = 16ULL * 1024ULL * 1024ULL * 1024ULL;
        initial_state.cards.push_back(card);
    }

    WorkloadBuilder workload_builder;
    auto requests = workload_builder.GenerateSimple(8, 1, 500);

    auto scheduler = std::make_unique<FIFOScheduler>();
    auto backend = std::make_unique<AnalyticalBackend>();

    Simulator sim(std::move(initial_state), std::move(scheduler), std::move(backend));
    sim.LoadWorkload(requests);
    sim.Run();
    sim.Report();
    return 0;
}
