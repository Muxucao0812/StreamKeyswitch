#pragma once

#include "model/stage.h"
#include "model/system_state.h"

#include <cstdint>
#include <vector>

enum class PrimitiveType {
    // Canonical primitive-level types for cycle simulation.
    // Data movement:
    KeyLoadHostToHBM,
    KeyLoadHBMToBRAM,
    InputHBMToBRAM,
    OutputBRAMToHBM,
    // Compute:
    Decompose,
    NTT,
    KSInnerProd,
    Accumulate,
    Subtract,
    INTT,
    BConv,
    // Cross-card communication / synchronization:
    InterCardSend,
    InterCardRecv,
    InterCardReduce,
    Barrier,

    // Legacy compatibility types. Keep until all call sites are migrated.
    KeyLoadDMA,
    DispatchDMA,
    DecomposeKernel,
    NttKernel,
    KSInnerProdKernel,
    InttKernel,
    AccumulateKernel,
    MultiplyKernel,
    BasisConvertKernel,
    InterCardComm,
    BarrierSync,
    MergeReduce
};

struct PrimitiveTileCoord {
    uint32_t ct_tile = 0;
    uint32_t limb_tile = 0;
    uint32_t digit_tile = 0;
};

struct PrimitiveOp {
    PrimitiveType type = PrimitiveType::DispatchDMA;
    StageType stage_type = StageType::Dispatch;

    uint64_t bytes = 0;
    uint64_t work_units = 0;

    // Endpoint metadata. -1 means unspecified.
    int32_t src_card = -1;
    int32_t dst_card = -1;
    uint32_t fan_in = 1;

    PrimitiveTileCoord tile;
    uint32_t sync_group = 0;
    uint32_t barrier_group = 0;
    // Optional pipeline stream/tag for overlapped scheduling semantics.
    // 0 means unspecified/default pipeline.
    uint32_t pipeline_tag = 0;

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

struct PrimitiveTypeBreakdown {
    PrimitiveType primitive_type = PrimitiveType::DispatchDMA;
    Time latency_ns = 0;
    double energy_nj = 0.0;
    uint64_t bytes = 0;
    uint64_t work_units = 0;
};

struct PrimitiveResult {
    Time total_latency_ns = 0;
    uint64_t peak_memory_bytes = 0;
    double total_energy_nj = 0.0;

    std::vector<PrimitiveStageBreakdown> stage_breakdown;
    std::vector<PrimitiveTypeBreakdown> primitive_breakdown;
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
