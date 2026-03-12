#pragma once

#include "model/stage.h"
#include "model/system_state.h"

#include <cstdint>
#include <vector>

enum class PrimitiveType {
    KeyLoadDMA,
    DispatchDMA,
    DecomposeKernel,
    MultiplyKernel,
    BasisConvertKernel,
    MergeReduce
};

struct PrimitiveOp {
    PrimitiveType type = PrimitiveType::DispatchDMA;
    StageType stage_type = StageType::Dispatch;

    uint64_t bytes = 0;
    uint32_t work_units = 0;

    std::vector<CardId> assigned_cards;
    bool key_hit = false;
};

struct PrimitiveTrace {
    std::vector<PrimitiveOp> ops;
};

struct PrimitiveStageBreakdown {
    StageType stage_type = StageType::Dispatch;
    Time latency_ns = 0;
    double energy_nj = 0.0;
};

struct PrimitiveResult {
    Time total_latency_ns = 0;
    uint64_t peak_memory_bytes = 0;
    double total_energy_nj = 0.0;

    std::vector<PrimitiveStageBreakdown> stage_breakdown;
};

class PrimitiveSimulatorStub {
public:
    PrimitiveResult Simulate(
        const PrimitiveTrace& trace,
        const SystemState& state) const;

private:
    Time EstimatePrimitiveLatency(const PrimitiveOp& op) const;
    double EstimatePrimitiveEnergy(const PrimitiveOp& op, Time latency_ns) const;
    uint64_t EstimatePrimitiveMemory(
        const PrimitiveOp& op,
        const SystemState& state) const;
};

const char* ToString(PrimitiveType primitive_type);
