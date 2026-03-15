#pragma once

#include "backend/model/keyswitch_method_policy.h"
#include "model/execution_result.h"
#include "model/keyswitch_reason.h"
#include "model/request.h"
#include "model/stage.h"
#include "model/system_state.h"

#include <cstddef>
#include <cstdint>
#include <vector>

struct KeySwitchProblem {
    bool valid = false;
    bool key_resident_hit = false;
    KeySwitchMethod method = KeySwitchMethod::Poseidon;

    uint32_t cards = 1;
    uint32_t ciphertexts = 1;
    uint32_t digits = 1;
    uint32_t limbs = 1;
    uint32_t polys = 1;
    uint32_t poly_modulus_degree = 1;

    uint64_t input_bytes = 0;
    uint64_t output_bytes = 0;
    uint64_t key_bytes = 0;

    uint64_t ct_limb_bytes = 1;
    uint64_t out_limb_bytes = 1;
    uint64_t key_digit_limb_bytes = 1;

    uint64_t min_card_bram_capacity_bytes = 0;
    uint64_t bram_budget_bytes = 0;
    uint64_t bram_guard_bytes = 0;
    double temp_buffer_ratio = 0.5;

    uint64_t working_set_bytes = 0;
};

struct BufferBreakdown {
    uint64_t key_buffer_bytes = 0;
    uint64_t ciphertext_buffer_bytes = 0;
    uint64_t temp_working_buffer_bytes = 0;
    uint64_t accumulation_buffer_bytes = 0;
};

struct BufferUsage {
    BufferBreakdown persistent;
    BufferBreakdown tile_static;
    BufferBreakdown dynamic;

    uint64_t persistent_bytes = 0;
    uint64_t static_bytes = 0;
    uint64_t dynamic_working_bytes = 0;

    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    uint64_t out_bytes = 0;
    uint64_t temp_bytes = 0;
    uint64_t total_live_bytes = 0;
};

struct PeakBufferUsage {
    BufferBreakdown component_peak;

    uint64_t persistent_peak_bytes = 0;
    uint64_t static_peak_bytes = 0;
    uint64_t dynamic_peak_bytes = 0;

    uint64_t key_peak_bytes = 0;
    uint64_t ct_peak_bytes = 0;
    uint64_t out_peak_bytes = 0;
    uint64_t temp_peak_bytes = 0;
    uint64_t total_peak_bytes = 0;
};

struct TileCandidate {
    bool valid = false;
    bool key_persistent = false;
    uint32_t ct_tile = 1;
    uint32_t limb_tile = 1;
    uint32_t digit_tile = 1;
    uint64_t score = 0;
    uint64_t estimated_peak_bram_bytes = 0;
    double estimated_total_cost = 0.0;
};

struct TileBufferUsage {
    uint32_t ct_tile_index = 0;
    uint32_t ct_count = 0;

    BufferBreakdown persistent_buffers;
    BufferBreakdown static_buffers;
    BufferBreakdown dynamic_peak_buffers;
    uint64_t persistent_bytes = 0;
    uint64_t static_bytes = 0;
    uint64_t dynamic_working_bytes = 0;

    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    uint64_t out_bytes = 0;
    uint64_t temp_bytes = 0;
    uint64_t peak_live_bytes = 0;
};

struct TileCostBreakdown {
    double tile_overhead_cost = 0.0;
    double key_transfer_cost = 0.0;
    double ct_transfer_cost = 0.0;
    double output_store_cost = 0.0;
    double decompose_compute_cost = 0.0;
    double multiply_compute_cost = 0.0;
    double basis_convert_cost = 0.0;
    double total_cost = 0.0;
};

struct TilePlan {
    bool valid = false;
    bool key_persistent = false;
    uint32_t ct_tile = 1;
    uint32_t limb_tile = 1;
    uint32_t digit_tile = 1;

    uint32_t ct_tiles = 0;
    uint32_t limb_tiles = 0;
    uint32_t digit_tiles = 0;
    uint32_t total_tile_count = 0;

    uint64_t estimated_peak_bram_bytes = 0;
    double estimated_total_cost = 0.0;
    TileCostBreakdown cost;
    std::vector<TileBufferUsage> per_tile_buffer_usage;
};

class TilePlanner {
public:
    struct Params {
        double bram_usable_ratio = 0.85;
        uint64_t bram_guard_bytes = 512ULL * 1024ULL;
        double temp_buffer_ratio = 0.5;
        bool allow_key_persistent = true;

        uint64_t hbm_to_bram_bw_bytes_per_ns = 32;
        uint64_t bram_to_hbm_bw_bytes_per_ns = 32;
        Time dma_setup_ns = 60;
        Time kernel_launch_ns = 80;
        uint64_t decompose_work_per_ns = 4096;
        uint64_t multiply_work_per_ns = 3072;
        uint64_t basis_work_per_ns = 2048;
        Time per_tile_fixed_overhead_ns = 20;

        double w_tile_overhead = 1.0;
        double w_key_transfer = 1.0;
        double w_ct_transfer = 1.0;
        double w_output_store = 1.0;
        double w_decompose_compute = 1.0;
        double w_multiply_compute = 1.0;
        double w_basis_convert = 1.0;
    };

    TilePlanner();
    explicit TilePlanner(const Params& params);

    TilePlan Plan(const KeySwitchProblem& problem) const;

private:
    Params params_;
};

enum class TransferDirection : uint8_t {
    HostToHBM,
    HBMToBRAM,
    BRAMToHBM
};

class TransferModel {
public:
    struct Params {
        uint64_t host_to_hbm_bw_bytes_per_ns = 32;
        uint64_t hbm_to_bram_bw_bytes_per_ns = 32;
        uint64_t bram_to_hbm_bw_bytes_per_ns = 32;
        Time host_to_hbm_setup_ns = 80;
        Time dma_setup_ns = 60;
        double energy_hbm_byte_nj = 0.0012;
    };

    TransferModel();
    explicit TransferModel(const Params& params);

    Time EstimateLatency(TransferDirection direction, uint64_t bytes) const;
    double EstimateEnergyByBytes(uint64_t bytes) const;

private:
    Params params_;
};

enum class TileExecutionStepType : uint8_t {
    KeyLoadHostToHBM,
    KeyLoadHBMToBRAM,
    InputHBMToBRAM,
    KeyHBMToBRAM,
    ModUpInttTile,
    ModUpBConvTile,
    ModUpNttTile,
    CrossDigitReduceTile,
    IntermediateBRAMToHBM,
    IntermediateHBMToBRAM,
    ModDownInttTile,
    ModDownBConvTile,
    ModDownNttTile,
    FinalSubtractTile,
    // Legacy single-board compatibility step types.
    DecomposeTile,
    NttTile,
    KSInnerProdTile,
    InttTile,
    AccumulateSubtractTile,
    BasisConvertTile,
    OutputBRAMToHBM,
    InterCardSendStep,
    InterCardRecvStep,
    InterCardReduceStep,
    BarrierStep,
    // Legacy compatibility step types.
    // They are reserved for compatibility adapters only and must not be emitted
    // by the method-aware main path (single-board / Cinnamon).
    InterCardCommTile,
    InterCardBarrier,
    Merge
};

inline constexpr std::size_t kTileExecutionStepTypeCount =
    static_cast<std::size_t>(TileExecutionStepType::Merge) + 1;

inline constexpr std::size_t ToIndex(TileExecutionStepType type) {
    return static_cast<std::size_t>(type);
}

struct TileExecutionStep {
    uint64_t step_id = 0;
    TileExecutionStepType type = TileExecutionStepType::InputHBMToBRAM;
    StageType stage_type = StageType::Dispatch;
    uint32_t tile_idx = 0;
    uint32_t digit_idx = 0;
    uint32_t limb_idx = 0;
    uint32_t ct_tile_index = 0;
    uint32_t limb_tile_index = 0;
    uint32_t digit_tile_index = 0;
    int32_t src_card = -1;
    int32_t dst_card = -1;
    uint32_t fan_in = 1;
    uint32_t sync_group = 0;
    uint32_t barrier_group = 0;
    uint64_t bytes = 0;
    uint64_t work_items = 0;
    uint64_t input_bytes = 0;
    uint64_t output_bytes = 0;
    IntermediateStorageLevel input_storage = IntermediateStorageLevel::SRAM;
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::SRAM;
    bool fused_with_prev = false;
    bool fused_with_next = false;
    bool is_shortcut_path = false;
    bool key_hit = false;
    bool key_persistent = false;
    std::vector<uint64_t> depends_on;
    BufferUsage before;
    BufferUsage after;
};

struct KeySwitchExecutionModelParams {
    TilePlanner::Params tile_planner;
    uint64_t default_bram_capacity_bytes = 32ULL * 1024ULL * 1024ULL;
};

struct KeySwitchExecution {
    bool valid = false;
    bool tiled_execution = false;
    bool fallback_used = false;
    KeySwitchFallbackReason fallback_reason = KeySwitchFallbackReason::None;
    KeySwitchMethod method = KeySwitchMethod::Poseidon;
    KeySwitchMethod requested_method = KeySwitchMethod::Poseidon;
    KeySwitchMethod effective_method = KeySwitchMethod::Poseidon;
    bool method_degraded = false;
    KeySwitchFallbackReason degraded_reason = KeySwitchFallbackReason::None;

    bool key_resident_hit = false;
    bool key_persistent_bram = false;

    uint64_t working_set_bytes = 0;
    uint32_t tile_count = 0;

    uint64_t key_host_to_hbm_bytes = 0;
    uint64_t key_hbm_to_bram_bytes = 0;
    uint64_t ct_hbm_to_bram_bytes = 0;
    uint64_t out_bram_to_hbm_bytes = 0;

    uint64_t peak_bram_bytes = 0;
    PeakBufferUsage peak_buffers;
    TileCostBreakdown tile_cost;
    KeySwitchMethodPolicy policy;

    KeySwitchProblem problem;
    TilePlan tile_plan;
    std::vector<TileExecutionStep> steps;
    std::vector<uint64_t> modup_step_ids;
    std::vector<uint64_t> innerprod_step_ids;
    std::vector<uint64_t> reduction_step_ids;
    std::vector<uint64_t> moddown_step_ids;
    uint64_t predicted_hbm_bytes = 0;
    uint64_t predicted_rf_peak = 0;
    uint64_t predicted_sram_peak = 0;
};

class KeySwitchExecutionModel {
public:
    KeySwitchExecutionModel();
    explicit KeySwitchExecutionModel(
        const KeySwitchExecutionModelParams& params);

    KeySwitchProblem BuildProblem(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    // Method-aware dispatcher.
    // Supported methods:
    // - Poseidon / OLA / FAB / FAST / HERA via the shared single-board builder
    // - Cinnamon
    // Unsupported methods are returned as explicit UnsupportedMethod fallback.
    KeySwitchExecution Build(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    // Single-board keyswitch builder path.
    // Used by Poseidon / OLA / FAB / FAST / HERA.
    KeySwitchExecution BuildSingleBoard(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

private:
    struct BuildContext {
        KeySwitchExecution* execution = nullptr;
        uint64_t next_step_id = 1;
        uint64_t rf_live_bytes = 0;
        uint64_t sram_live_bytes = 0;
        uint64_t rf_peak_bytes = 0;
        uint64_t sram_peak_bytes = 0;
    };

    uint64_t AppendStep(
        BuildContext* ctx,
        TileExecutionStep step) const;

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

    void FinalizeExecution(
        BuildContext* ctx) const;

    // Method dispatcher helper.
    // Returns a fully formed execution for supported methods, or explicit
    // unsupported fallback for unimplemented methods.
    KeySwitchExecution BuildByMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod resolved_method) const;

    bool ResidentKeyHit(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    KeySwitchExecution BuildWithMode(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        bool allow_inter_card_steps) const;

    KeySwitchExecution BuildUnsupportedMethod(
        KeySwitchMethod requested_method,
        KeySwitchMethod effective_method
    ) const;

    KeySwitchExecution BuildSharedSingleBoardMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod method) const;

    KeySwitchExecution BuildCinnamon(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

private:
    KeySwitchExecutionModelParams params_;
    TilePlanner planner_;
};
