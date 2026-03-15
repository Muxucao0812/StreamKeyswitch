#pragma once

#include "backend/hw/hardware_model.h"
#include "backend/runtime_planner.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

struct TensorHandle {
    uint64_t id = 0;
    uint64_t producer_step_id = 0;
    uint64_t bytes = 0;
    IntermediateStorageLevel storage = IntermediateStorageLevel::BRAM;
    uint32_t remaining_uses = 0;
    bool alive = false;
    bool materialized = true;
    bool persistent = false;
};

struct StepRuntimeRecord {
    uint64_t step_id = 0;
    TileExecutionStepType step_type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint64_t input_bytes = 0;
    uint64_t output_bytes = 0;
    uint64_t transfer_bytes = 0;
    uint64_t compute_cycles = 0;
    uint64_t transfer_cycles = 0;
    uint64_t total_cycles = 0;
    uint64_t live_bram_bytes_before = 0;
    uint64_t live_hbm_bytes_before = 0;
    uint64_t live_bram_bytes_after = 0;
    uint64_t live_hbm_bytes_after = 0;
    bool used_direct_forward = false;
    bool is_spill = false;
    bool is_reload = false;
};

struct MoveResult {
    uint64_t bytes = 0;
    uint64_t cycles = 0;
    bool direct_forward = false;
};

struct RuntimeState {
    bool valid = false;
    uint64_t live_bram_bytes = 0;
    uint64_t live_hbm_bytes = 0;
    uint64_t peak_bram_bytes = 0;
    uint64_t peak_hbm_bytes = 0;
    uint64_t bram_read_bytes = 0;
    uint64_t bram_write_bytes = 0;
    uint64_t hbm_read_bytes = 0;
    uint64_t hbm_write_bytes = 0;
    uint64_t compute_cycles = 0;
    uint64_t transfer_cycles = 0;
    uint64_t total_cycles = 0;
    uint64_t direct_forward_count = 0;
    uint64_t direct_forward_bytes = 0;
    uint64_t spill_count = 0;
    uint64_t reload_count = 0;
    uint64_t spill_bytes = 0;
    uint64_t reload_bytes = 0;
    std::array<uint64_t, kTileExecutionStepTypeCount> fine_step_cycles{};
    std::unordered_map<uint64_t, TensorHandle> tensors;
    std::vector<StepRuntimeRecord> step_records;
};

class StepGraphRuntimeExecutor {
public:
    explicit StepGraphRuntimeExecutor(const HardwareModel& hardware);

    RuntimeState ExecuteStepGraph(const RuntimePlan& plan) const;
    RuntimeState ExecuteStepGraph(const KeySwitchExecution& execution) const;
    MoveResult MoveOrForward(
        const TileExecutionStep& step,
        const TensorHandle& input,
        RuntimeState* state) const;

private:
    void ExecINTT(
        const TileExecutionStep& step,
        const std::vector<const TensorHandle*>& inputs,
        const RuntimePlan& plan,
        StepRuntimeRecord* record,
        RuntimeState* state) const;
    void ExecBConv(
        const TileExecutionStep& step,
        const std::vector<const TensorHandle*>& inputs,
        const RuntimePlan& plan,
        StepRuntimeRecord* record,
        RuntimeState* state) const;
    void ExecNTT(
        const TileExecutionStep& step,
        const std::vector<const TensorHandle*>& inputs,
        const RuntimePlan& plan,
        StepRuntimeRecord* record,
        RuntimeState* state) const;
    void ExecInnerProd(
        const TileExecutionStep& step,
        const std::vector<const TensorHandle*>& inputs,
        const RuntimePlan& plan,
        StepRuntimeRecord* record,
        RuntimeState* state) const;
    void ExecReduce(
        const TileExecutionStep& step,
        const std::vector<const TensorHandle*>& inputs,
        const RuntimePlan& plan,
        StepRuntimeRecord* record,
        RuntimeState* state) const;
    void ExecSubtract(
        const TileExecutionStep& step,
        const std::vector<const TensorHandle*>& inputs,
        const RuntimePlan& plan,
        StepRuntimeRecord* record,
        RuntimeState* state) const;

private:
    const HardwareModel& hardware_;
};
