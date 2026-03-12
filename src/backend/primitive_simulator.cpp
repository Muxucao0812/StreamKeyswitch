#include "backend/primitive_simulator.h"

#include <algorithm>
#include <unordered_map>

namespace {

uint32_t CardCount(const PrimitiveOp& op) {
    return static_cast<uint32_t>(std::max<size_t>(1, op.assigned_cards.size()));
}

PrimitiveStageBreakdown* FindStageBreakdown(
    std::vector<PrimitiveStageBreakdown>* breakdown,
    StageType stage_type) {

    for (auto& entry : *breakdown) {
        if (entry.stage_type == stage_type) {
            return &entry;
        }
    }

    breakdown->push_back(PrimitiveStageBreakdown{stage_type, 0, 0.0});
    return &breakdown->back();
}

} // namespace

const char* ToString(PrimitiveType primitive_type) {
    switch (primitive_type) {
    case PrimitiveType::KeyLoadDMA:
        return "KeyLoadDMA";
    case PrimitiveType::DispatchDMA:
        return "DispatchDMA";
    case PrimitiveType::DecomposeKernel:
        return "DecomposeKernel";
    case PrimitiveType::MultiplyKernel:
        return "MultiplyKernel";
    case PrimitiveType::BasisConvertKernel:
        return "BasisConvertKernel";
    case PrimitiveType::MergeReduce:
        return "MergeReduce";
    }

    return "DispatchDMA";
}

Time PrimitiveSimulatorStub::EstimatePrimitiveLatency(const PrimitiveOp& op) const {
    const uint32_t cards = CardCount(op);

    switch (op.type) {
    case PrimitiveType::KeyLoadDMA:
        return static_cast<Time>(op.bytes / 1024ULL)
            + 320
            + (op.key_hit ? 20 : 160);

    case PrimitiveType::DispatchDMA:
        return static_cast<Time>(op.bytes / 4096ULL)
            + 45
            + (cards > 1 ? (10 * static_cast<Time>(cards - 1)) : 0);

    case PrimitiveType::DecomposeKernel:
        return (24 * static_cast<Time>(op.work_units)) / cards
            + 260
            + (cards > 1 ? (8 * static_cast<Time>(cards - 1)) : 0);

    case PrimitiveType::MultiplyKernel:
        return (34 * static_cast<Time>(op.work_units)) / cards
            + 420
            + (cards > 1 ? (14 * static_cast<Time>(cards - 1)) : 0);

    case PrimitiveType::BasisConvertKernel:
        return (19 * static_cast<Time>(op.work_units)) / cards
            + 180
            + (cards > 1 ? (7 * static_cast<Time>(cards - 1)) : 0);

    case PrimitiveType::MergeReduce:
        if (cards <= 1) {
            return 0;
        }
        return static_cast<Time>(op.bytes / 8192ULL)
            + 140
            + (36 * static_cast<Time>(cards));
    }

    return 0;
}

double PrimitiveSimulatorStub::EstimatePrimitiveEnergy(
    const PrimitiveOp& op,
    Time latency_ns) const {

    double scale = 1.0;
    switch (op.type) {
    case PrimitiveType::KeyLoadDMA:
        scale = 0.22;
        break;
    case PrimitiveType::DispatchDMA:
        scale = 0.25;
        break;
    case PrimitiveType::DecomposeKernel:
        scale = 0.48;
        break;
    case PrimitiveType::MultiplyKernel:
        scale = 0.62;
        break;
    case PrimitiveType::BasisConvertKernel:
        scale = 0.44;
        break;
    case PrimitiveType::MergeReduce:
        scale = 0.35;
        break;
    }

    return static_cast<double>(latency_ns) * scale;
}

uint64_t PrimitiveSimulatorStub::EstimatePrimitiveMemory(
    const PrimitiveOp& op,
    const SystemState& state) const {

    uint64_t base = op.bytes;
    if (base == 0) {
        base = static_cast<uint64_t>(op.work_units) * 256ULL;
    }

    const uint32_t cards = CardCount(op);
    if (cards > 1) {
        base = (base / cards) + static_cast<uint64_t>(cards - 1) * 4096ULL;
    }

    uint64_t min_capacity = 0;
    for (const CardId card_id : op.assigned_cards) {
        if (card_id < state.cards.size()) {
            const uint64_t cap = state.cards[card_id].memory_capacity_bytes;
            if (min_capacity == 0 || cap < min_capacity) {
                min_capacity = cap;
            }
        }
    }

    if (min_capacity == 0) {
        return base;
    }

    return std::min<uint64_t>(base, min_capacity);
}

PrimitiveResult PrimitiveSimulatorStub::Simulate(
    const PrimitiveTrace& trace,
    const SystemState& state) const {

    PrimitiveResult result;

    for (const PrimitiveOp& op : trace.ops) {
        const Time latency_ns = EstimatePrimitiveLatency(op);
        const double energy_nj = EstimatePrimitiveEnergy(op, latency_ns);
        const uint64_t memory_bytes = EstimatePrimitiveMemory(op, state);

        result.total_latency_ns += latency_ns;
        result.total_energy_nj += energy_nj;
        result.peak_memory_bytes = std::max(result.peak_memory_bytes, memory_bytes);

        PrimitiveStageBreakdown* stage_entry =
            FindStageBreakdown(&result.stage_breakdown, op.stage_type);
        stage_entry->latency_ns += latency_ns;
        stage_entry->energy_nj += energy_nj;
    }

    return result;
}
