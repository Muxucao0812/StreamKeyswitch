#pragma once

#include "backend/hw/hardware_model.h"
#include "backend/model/keyswitch_execution_model.h"

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

struct RuntimePlan {
    bool valid = false;
    KeySwitchProblem problem;
    KeySwitchMethodPolicy policy;
    TilePlan tile_plan;
    std::vector<TileExecutionStep> steps;

    uint32_t tile_count = 0;
    bool key_resident_hit = false;
    bool key_persistent_bram = false;

    uint64_t working_set_bytes = 0;
    uint64_t key_host_to_hbm_bytes = 0;
    uint64_t key_hbm_to_bram_bytes = 0;
    uint64_t ct_hbm_to_bram_bytes = 0;
    uint64_t out_bram_to_hbm_bytes = 0;
};

const char* ToString(TileExecutionStepType type);
const char* ToString(IntermediateStorageLevel storage);
void DumpRuntimePlan(const RuntimePlan& plan, std::ostream& os);
std::string FormatRuntimePlan(const RuntimePlan& plan);

class RuntimePlanner {
public:
    RuntimePlanner(
        const HardwareModel& hardware,
        const TilePlanner::Params& tile_params);

    RuntimePlan Plan(const KeySwitchExecution& execution) const;

private:
    struct BuildContext {
        RuntimePlan* plan = nullptr;
        uint64_t next_step_id = 1;
    };

    TileExecutionStep MakeStep(
        TileExecutionStepType type,
        StageType stage_type,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        uint64_t input_bytes,
        uint64_t output_bytes,
        uint64_t work_items,
        IntermediateStorageLevel input_storage,
        IntermediateStorageLevel output_storage) const;

    uint64_t AppendStep(BuildContext* ctx, TileExecutionStep step) const;

    std::vector<uint64_t> BuildModUpSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        uint64_t ct_chunk_bytes,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps) const;

    std::vector<uint64_t> BuildInnerProdSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        uint64_t key_chunk_bytes,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps) const;

    std::vector<uint64_t> BuildReductionSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps) const;

    std::vector<uint64_t> BuildModDownSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint64_t out_bytes,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps) const;

    std::vector<uint64_t> ConnectSubgraphsWithPolicy(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        StageConnectionMode mode,
        uint64_t bytes,
        const std::vector<uint64_t>& deps) const;

private:
    TilePlanner planner_;
};
