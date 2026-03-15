#include "backend/runtime_planner.h"

#include <algorithm>
#include <ostream>
#include <sstream>

namespace {

IntermediateStorageLevel CanonicalizeOnChipStorage(IntermediateStorageLevel storage) {
    return (storage == IntermediateStorageLevel::HBM)
        ? IntermediateStorageLevel::HBM
        : IntermediateStorageLevel::BRAM;
}

IntermediateStorageLevel StorageForConnection(
    StageConnectionMode mode,
    IntermediateStorageLevel fallback_level) {

    switch (mode) {
    case StageConnectionMode::DirectForward:
        return CanonicalizeOnChipStorage(fallback_level);
    case StageConnectionMode::BufferInBRAM:
        return IntermediateStorageLevel::BRAM;
    case StageConnectionMode::SpillToHBM:
        return IntermediateStorageLevel::HBM;
    }
    return CanonicalizeOnChipStorage(fallback_level);
}

std::vector<uint32_t> IterationOrder(uint32_t count) {
    std::vector<uint32_t> order;
    order.reserve(count);
    for (uint32_t idx = 0; idx < count; ++idx) {
        order.push_back(idx);
    }
    return order;
}

void AddDependencyId(
    std::vector<uint64_t>* deps,
    uint64_t step_id) {

    if (step_id == 0) {
        return;
    }
    if (std::find(deps->begin(), deps->end(), step_id) == deps->end()) {
        deps->push_back(step_id);
    }
}

bool HasLogicalNode(const LogicalGraph& graph, LogicalNodeKind kind) {
    for (const LogicalNode& node : graph.nodes) {
        if (node.kind == kind) {
            return true;
        }
    }
    return false;
}

void AppendCommaSeparatedU64(
    const std::vector<uint64_t>& values,
    std::ostream& os) {

    for (std::size_t idx = 0; idx < values.size(); ++idx) {
        if (idx != 0) {
            os << ", ";
        }
        os << values[idx];
    }
}

} // namespace

const char* ToString(TileExecutionStepType type) {
    switch (type) {
    case TileExecutionStepType::KeyLoadHostToHBM:
        return "KeyLoadHostToHBM";
    case TileExecutionStepType::KeyLoadHBMToBRAM:
        return "KeyLoadHBMToBRAM";
    case TileExecutionStepType::InputHBMToBRAM:
        return "InputHBMToBRAM";
    case TileExecutionStepType::KeyHBMToBRAM:
        return "KeyHBMToBRAM";
    case TileExecutionStepType::ModUpInttTile:
        return "ModUpInttTile";
    case TileExecutionStepType::ModUpBConvTile:
        return "ModUpBConvTile";
    case TileExecutionStepType::ModUpNttTile:
        return "ModUpNttTile";
    case TileExecutionStepType::KSInnerProdTile:
        return "KSInnerProdTile";
    case TileExecutionStepType::CrossDigitReduceTile:
        return "CrossDigitReduceTile";
    case TileExecutionStepType::IntermediateBRAMToHBM:
        return "IntermediateBRAMToHBM";
    case TileExecutionStepType::IntermediateHBMToBRAM:
        return "IntermediateHBMToBRAM";
    case TileExecutionStepType::ModDownInttTile:
        return "ModDownInttTile";
    case TileExecutionStepType::ModDownBConvTile:
        return "ModDownBConvTile";
    case TileExecutionStepType::ModDownNttTile:
        return "ModDownNttTile";
    case TileExecutionStepType::FinalSubtractTile:
        return "FinalSubtractTile";
    case TileExecutionStepType::OutputBRAMToHBM:
        return "OutputBRAMToHBM";
    case TileExecutionStepType::InterCardSendStep:
        return "InterCardSendStep";
    case TileExecutionStepType::InterCardRecvStep:
        return "InterCardRecvStep";
    case TileExecutionStepType::InterCardReduceStep:
        return "InterCardReduceStep";
    case TileExecutionStepType::BarrierStep:
        return "BarrierStep";
    case TileExecutionStepType::DecomposeTile:
        return "DecomposeTile";
    case TileExecutionStepType::NttTile:
        return "NttTile";
    case TileExecutionStepType::InttTile:
        return "InttTile";
    case TileExecutionStepType::BasisConvertTile:
        return "BasisConvertTile";
    case TileExecutionStepType::AccumulateSubtractTile:
        return "AccumulateSubtractTile";
    case TileExecutionStepType::InterCardCommTile:
        return "InterCardCommTile";
    case TileExecutionStepType::InterCardBarrier:
        return "InterCardBarrier";
    case TileExecutionStepType::Merge:
        return "Merge";
    }
    return "Unknown";
}

const char* ToString(IntermediateStorageLevel storage) {
    switch (storage) {
    case IntermediateStorageLevel::BRAM:
        return "BRAM";
    case IntermediateStorageLevel::HBM:
        return "HBM";
    }
    return "Unknown";
}

void DumpRuntimePlan(const RuntimePlan& plan, std::ostream& os) {
    os << "RuntimePlan(valid=" << (plan.valid ? "true" : "false")
       << ", tile_count=" << plan.tile_count
       << ", ct_tile=" << plan.tile_plan.ct_tile
       << ", limb_tile=" << plan.tile_plan.limb_tile
       << ", digit_tile=" << plan.tile_plan.digit_tile
       << ", key_persistent=" << (plan.key_persistent_bram ? "true" : "false")
       << ", steps=" << plan.steps.size() << ")\n";
    for (const TileExecutionStep& step : plan.steps) {
        os << "  [" << step.step_id << "] "
           << ToString(step.type)
           << " stage=" << ToString(step.stage_type)
           << " ct=" << step.ct_tile_index
           << " limb=" << step.limb_tile_index
           << " digit=" << step.digit_tile_index
           << " in=" << step.input_bytes
           << " out=" << step.output_bytes
           << " work=" << step.work_items
           << " input_storage=" << ToString(step.input_storage)
           << " output_storage=" << ToString(step.output_storage)
           << " deps=[";
        AppendCommaSeparatedU64(step.depends_on, os);
        os << "] fused_prev=" << (step.fused_with_prev ? "true" : "false")
           << " fused_next=" << (step.fused_with_next ? "true" : "false")
           << " materialize=" << (step.materialize_output ? "true" : "false")
           << " shortcut=" << (step.is_shortcut_path ? "true" : "false")
           << "\n";
    }
}

std::string FormatRuntimePlan(const RuntimePlan& plan) {
    std::ostringstream os;
    DumpRuntimePlan(plan, os);
    return os.str();
}

RuntimePlanner::RuntimePlanner(
    const HardwareModel& hardware,
    const TilePlanner::Params& tile_params)
    : planner_(tile_params) {
    (void)hardware;
}

TileExecutionStep RuntimePlanner::MakeStep(
    TileExecutionStepType type,
    StageType stage_type,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    uint64_t input_bytes,
    uint64_t output_bytes,
    uint64_t work_items,
    IntermediateStorageLevel input_storage,
    IntermediateStorageLevel output_storage) const {

    TileExecutionStep step;
    step.type = type;
    step.stage_type = stage_type;
    step.ct_tile_index = ct_idx;
    step.limb_tile_index = limb_idx;
    step.digit_tile_index = digit_idx;
    step.ct_idx = ct_idx;
    step.tile_idx = ct_idx;
    step.digit_idx = digit_idx;
    step.limb_idx = limb_idx;
    step.input_bytes = input_bytes;
    step.output_bytes = output_bytes;
    step.work_items = work_items;
    step.input_storage = CanonicalizeOnChipStorage(input_storage);
    step.output_storage = CanonicalizeOnChipStorage(output_storage);
    step.fused_with_prev = false;
    step.fused_with_next = false;
    step.is_shortcut_path = false;
    return step;
}

uint64_t RuntimePlanner::AppendStep(
    BuildContext* ctx,
    TileExecutionStep step) const {

    step.step_id = ctx->next_step_id++;
    if (step.fused_with_next) {
        step.materialize_output = false;
    }
    if (step.input_buffer_ids.empty()) {
        step.input_buffer_ids = step.depends_on;
    }
    step.output_buffer_id = step.step_id;
    step.bytes = (step.bytes == 0) ? std::max(step.input_bytes, step.output_bytes) : step.bytes;
    ctx->plan->steps.push_back(std::move(step));
    return ctx->plan->steps.back().step_id;
}

std::vector<uint64_t> RuntimePlanner::BuildModUpSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    uint64_t ct_chunk_bytes,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    std::vector<uint64_t> ids;

    TileExecutionStep intt = MakeStep(
        TileExecutionStepType::ModUpInttTile,
        StageType::BasisConvert,
        ct_idx,
        limb_idx,
        digit_idx,
        ct_chunk_bytes,
        ct_chunk_bytes,
        work_items,
        IntermediateStorageLevel::BRAM,
        IntermediateStorageLevel::BRAM);
    intt.fused_with_next = policy.fuse_modup_chain;
    intt.depends_on = deps;
    ids.push_back(AppendStep(ctx, intt));

    TileExecutionStep bconv = MakeStep(
        TileExecutionStepType::ModUpBConvTile,
        StageType::BasisConvert,
        ct_idx,
        limb_idx,
        digit_idx,
        ct_chunk_bytes,
        ct_chunk_bytes,
        work_items,
        intt.output_storage,
        IntermediateStorageLevel::BRAM);
    bconv.fused_with_prev = policy.fuse_modup_chain;
    bconv.fused_with_next = policy.fuse_modup_chain;
    bconv.depends_on = {ids.back()};
    ids.push_back(AppendStep(ctx, bconv));

    TileExecutionStep ntt = MakeStep(
        TileExecutionStepType::ModUpNttTile,
        StageType::BasisConvert,
        ct_idx,
        limb_idx,
        digit_idx,
        ct_chunk_bytes,
        ct_chunk_bytes,
        work_items,
        bconv.output_storage,
        policy.modup_output_storage);
    ntt.fused_with_prev = policy.fuse_modup_chain;
    ntt.fused_with_next =
        (policy.modup_to_innerprod == StageConnectionMode::DirectForward);
    ntt.depends_on = {ids.back()};
    ids.push_back(AppendStep(ctx, ntt));
    return ids;
}

std::vector<uint64_t> RuntimePlanner::BuildInnerProdSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    uint64_t key_chunk_bytes,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    std::vector<uint64_t> ids;
    TileExecutionStep inner = MakeStep(
        TileExecutionStepType::KSInnerProdTile,
        StageType::Multiply,
        ct_idx,
        limb_idx,
        digit_idx,
        key_chunk_bytes,
        key_chunk_bytes,
        work_items,
        StorageForConnection(policy.modup_to_innerprod, policy.modup_output_storage),
        policy.innerprod_output_storage);
    inner.fused_with_prev = policy.fuse_cross_stage;
    inner.depends_on = deps;
    ids.push_back(AppendStep(ctx, inner));
    return ids;
}

std::vector<uint64_t> RuntimePlanner::BuildReductionSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    std::vector<uint64_t> ids;
    TileExecutionStep reduce = MakeStep(
        TileExecutionStepType::CrossDigitReduceTile,
        StageType::Multiply,
        ct_idx,
        limb_idx,
        0,
        std::max<uint64_t>(1, work_items),
        std::max<uint64_t>(1, work_items),
        work_items,
        StorageForConnection(policy.innerprod_to_reduction, policy.innerprod_output_storage),
        policy.reduction_output_storage);
    reduce.depends_on = deps;
    ids.push_back(AppendStep(ctx, reduce));
    return ids;
}

std::vector<uint64_t> RuntimePlanner::BuildModDownSubgraph(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint64_t out_bytes,
    uint64_t work_items,
    const KeySwitchMethodPolicy& policy,
    const std::vector<uint64_t>& deps) const {

    const uint64_t shortcut_bytes = policy.supports_moddown_shortcut
        ? std::max<uint64_t>(1, out_bytes / 2)
        : out_bytes;
    const uint64_t shortcut_work = policy.supports_moddown_shortcut
        ? std::max<uint64_t>(1, work_items / 2)
        : work_items;

    std::vector<uint64_t> ids;

    TileExecutionStep intt = MakeStep(
        TileExecutionStepType::ModDownInttTile,
        StageType::BasisConvert,
        ct_idx,
        limb_idx,
        0,
        shortcut_bytes,
        shortcut_bytes,
        shortcut_work,
        StorageForConnection(policy.reduction_to_moddown, policy.reduction_output_storage),
        policy.fuse_moddown_chain ? policy.moddown_temp_storage : IntermediateStorageLevel::BRAM);
    intt.fused_with_next = policy.fuse_moddown_chain;
    intt.depends_on = deps;
    intt.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, intt));

    TileExecutionStep bconv = MakeStep(
        TileExecutionStepType::ModDownBConvTile,
        StageType::BasisConvert,
        ct_idx,
        limb_idx,
        0,
        shortcut_bytes,
        shortcut_bytes,
        shortcut_work,
        intt.output_storage,
        policy.fuse_moddown_chain ? policy.moddown_temp_storage : IntermediateStorageLevel::BRAM);
    bconv.fused_with_prev = policy.fuse_moddown_chain;
    bconv.fused_with_next = policy.fuse_moddown_chain;
    bconv.depends_on = {ids.back()};
    bconv.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, bconv));

    TileExecutionStep ntt = MakeStep(
        TileExecutionStepType::ModDownNttTile,
        StageType::BasisConvert,
        ct_idx,
        limb_idx,
        0,
        shortcut_bytes,
        shortcut_bytes,
        shortcut_work,
        bconv.output_storage,
        policy.moddown_temp_storage);
    ntt.fused_with_prev = policy.fuse_moddown_chain;
    ntt.fused_with_next =
        (policy.moddown_to_subtract == StageConnectionMode::DirectForward);
    ntt.depends_on = {ids.back()};
    ntt.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, ntt));

    TileExecutionStep sub = MakeStep(
        TileExecutionStepType::FinalSubtractTile,
        StageType::Multiply,
        ct_idx,
        limb_idx,
        0,
        out_bytes,
        out_bytes,
        work_items,
        StorageForConnection(policy.moddown_to_subtract, policy.moddown_temp_storage),
        IntermediateStorageLevel::BRAM);
    sub.fused_with_prev =
        (policy.moddown_to_subtract == StageConnectionMode::DirectForward);
    sub.depends_on = {ids.back()};
    sub.is_shortcut_path = policy.supports_moddown_shortcut;
    ids.push_back(AppendStep(ctx, sub));

    return ids;
}

std::vector<uint64_t> RuntimePlanner::ConnectSubgraphsWithPolicy(
    BuildContext* ctx,
    uint32_t ct_idx,
    uint32_t limb_idx,
    uint32_t digit_idx,
    StageConnectionMode mode,
    uint64_t bytes,
    const std::vector<uint64_t>& deps) const {

    switch (mode) {
    case StageConnectionMode::DirectForward:
    case StageConnectionMode::BufferInBRAM:
        return deps;
    case StageConnectionMode::SpillToHBM:
        break;
    }

    TileExecutionStep spill = MakeStep(
        TileExecutionStepType::IntermediateBRAMToHBM,
        StageType::Dispatch,
        ct_idx,
        limb_idx,
        digit_idx,
        bytes,
        bytes,
        1,
        IntermediateStorageLevel::BRAM,
        IntermediateStorageLevel::HBM);
    spill.bytes = bytes;
    spill.depends_on = deps;
    const uint64_t spill_id = AppendStep(ctx, spill);

    TileExecutionStep reload = MakeStep(
        TileExecutionStepType::IntermediateHBMToBRAM,
        StageType::Dispatch,
        ct_idx,
        limb_idx,
        digit_idx,
        bytes,
        bytes,
        1,
        IntermediateStorageLevel::HBM,
        IntermediateStorageLevel::BRAM);
    reload.bytes = bytes;
    reload.depends_on = {spill_id};
    const uint64_t reload_id = AppendStep(ctx, reload);

    ctx->plan->out_bram_to_hbm_bytes += bytes;
    ctx->plan->ct_hbm_to_bram_bytes += bytes;
    return {reload_id};
}

RuntimePlan RuntimePlanner::Plan(const KeySwitchExecution& execution) const {
    RuntimePlan plan;
    plan.problem = execution.problem;
    plan.policy = execution.policy;
    plan.key_resident_hit = execution.key_resident_hit;
    plan.working_set_bytes = execution.working_set_bytes;

    if (!execution.valid || !execution.logical_graph.valid || execution.problem.cards > 1) {
        return plan;
    }
    if (!HasLogicalNode(execution.logical_graph, LogicalNodeKind::ModUp)
        || !HasLogicalNode(execution.logical_graph, LogicalNodeKind::InnerProd)
        || !HasLogicalNode(execution.logical_graph, LogicalNodeKind::ModDown)) {
        return plan;
    }

    plan.tile_plan = planner_.Plan(execution.problem);
    if (!plan.tile_plan.valid) {
        return plan;
    }

    plan.valid = true;
    plan.key_persistent_bram = plan.tile_plan.key_persistent;
    plan.tile_count = plan.tile_plan.total_tile_count;

    BuildContext ctx;
    ctx.plan = &plan;

    uint64_t host_key_step_id = 0;
    if (!plan.problem.key_resident_hit) {
        TileExecutionStep host_key = MakeStep(
            TileExecutionStepType::KeyLoadHostToHBM,
            StageType::KeyLoad,
            0,
            0,
            0,
            plan.problem.key_bytes,
            plan.problem.key_bytes,
            1,
            IntermediateStorageLevel::HBM,
            IntermediateStorageLevel::HBM);
        host_key.key_hit = false;
        host_key.key_persistent = false;
        host_key.bytes = plan.problem.key_bytes;
        host_key_step_id = AppendStep(&ctx, host_key);
        plan.key_host_to_hbm_bytes += plan.problem.key_bytes;
    }

    uint64_t persistent_key_step_id = 0;
    if (plan.tile_plan.key_persistent) {
        TileExecutionStep persistent_key = MakeStep(
            TileExecutionStepType::KeyHBMToBRAM,
            StageType::KeyLoad,
            0,
            0,
            0,
            plan.problem.key_bytes,
            plan.problem.key_bytes,
            1,
            IntermediateStorageLevel::HBM,
            plan.policy.key_pref_storage);
        persistent_key.key_hit = true;
        persistent_key.key_persistent = true;
        if (host_key_step_id != 0) {
            persistent_key.depends_on.push_back(host_key_step_id);
        }
        persistent_key.bytes = plan.problem.key_bytes;
        persistent_key_step_id = AppendStep(&ctx, persistent_key);
        plan.key_hbm_to_bram_bytes += plan.problem.key_bytes;
    }

    const uint32_t ct_tiles = std::max<uint32_t>(1, plan.tile_plan.ct_tiles);
    const uint32_t limb_tiles = std::max<uint32_t>(1, plan.tile_plan.limb_tiles);
    const uint32_t digit_tiles = std::max<uint32_t>(1, plan.tile_plan.digit_tiles);
    const std::vector<uint32_t> limb_order = IterationOrder(limb_tiles);
    const std::vector<uint32_t> digit_order = IterationOrder(digit_tiles);

    for (uint32_t ct_idx = 0; ct_idx < ct_tiles; ++ct_idx) {
        const uint32_t ct_remain =
            plan.problem.ciphertexts - ct_idx * plan.tile_plan.ct_tile;
        const uint32_t ct_now = std::min<uint32_t>(plan.tile_plan.ct_tile, ct_remain);

        std::vector<uint64_t> input_step_by_limb(limb_tiles, 0);
        std::vector<std::vector<uint64_t>> inner_terminal(
            limb_tiles,
            std::vector<uint64_t>(digit_tiles, 0));

        auto ensure_input_step = [&](uint32_t limb_idx) {
            if (input_step_by_limb[limb_idx] != 0) {
                return input_step_by_limb[limb_idx];
            }

            const uint32_t limb_remain =
                plan.problem.limbs - limb_idx * plan.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(plan.tile_plan.limb_tile, limb_remain);
            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * plan.problem.ct_limb_bytes;

            TileExecutionStep input = MakeStep(
                TileExecutionStepType::InputHBMToBRAM,
                StageType::Dispatch,
                ct_idx,
                limb_idx,
                0,
                ct_chunk_bytes,
                ct_chunk_bytes,
                1,
                IntermediateStorageLevel::HBM,
                plan.policy.input_pref_storage);
            input.bytes = ct_chunk_bytes;
            input_step_by_limb[limb_idx] = AppendStep(&ctx, input);
            plan.ct_hbm_to_bram_bytes += ct_chunk_bytes;
            return input_step_by_limb[limb_idx];
        };

        auto build_for_pair = [&](uint32_t limb_idx, uint32_t digit_idx) {
            const uint32_t limb_remain =
                plan.problem.limbs - limb_idx * plan.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(plan.tile_plan.limb_tile, limb_remain);
            const uint32_t digit_remain =
                plan.problem.digits - digit_idx * plan.tile_plan.digit_tile;
            const uint32_t digit_now = std::min<uint32_t>(plan.tile_plan.digit_tile, digit_remain);

            const uint64_t ct_chunk_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * plan.problem.ct_limb_bytes;
            const uint64_t key_chunk_bytes =
                static_cast<uint64_t>(limb_now) * digit_now * plan.problem.key_digit_limb_bytes;
            const uint64_t decompose_work =
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(digit_now)
                * static_cast<uint64_t>(plan.problem.poly_modulus_degree);
            const uint64_t inner_work =
                decompose_work * static_cast<uint64_t>(plan.problem.polys);

            const std::vector<uint64_t> modup_ids = BuildModUpSubgraph(
                &ctx,
                ct_idx,
                limb_idx,
                digit_idx,
                ct_chunk_bytes,
                decompose_work,
                plan.policy,
                {ensure_input_step(limb_idx)});

            std::vector<uint64_t> inner_deps = ConnectSubgraphsWithPolicy(
                &ctx,
                ct_idx,
                limb_idx,
                digit_idx,
                plan.policy.modup_to_innerprod,
                ct_chunk_bytes,
                {modup_ids.back()});

            if (plan.tile_plan.key_persistent && persistent_key_step_id != 0) {
                AddDependencyId(&inner_deps, persistent_key_step_id);
            } else {
                TileExecutionStep key_load = MakeStep(
                    TileExecutionStepType::KeyHBMToBRAM,
                    StageType::KeyLoad,
                    ct_idx,
                    limb_idx,
                    digit_idx,
                    key_chunk_bytes,
                    key_chunk_bytes,
                    1,
                    IntermediateStorageLevel::HBM,
                    plan.policy.key_pref_storage);
                key_load.bytes = key_chunk_bytes;
                if (host_key_step_id != 0) {
                    key_load.depends_on.push_back(host_key_step_id);
                }
                const uint64_t key_load_id = AppendStep(&ctx, key_load);
                plan.key_hbm_to_bram_bytes += key_chunk_bytes;
                AddDependencyId(&inner_deps, key_load_id);
            }

            const std::vector<uint64_t> inner_ids = BuildInnerProdSubgraph(
                &ctx,
                ct_idx,
                limb_idx,
                digit_idx,
                key_chunk_bytes,
                inner_work,
                plan.policy,
                inner_deps);
            inner_terminal[limb_idx][digit_idx] = inner_ids.back();
        };

        if (plan.policy.granularity == KeySwitchProcessingGranularity::Digit
            || plan.policy.prefer_digit_locality) {
            for (uint32_t digit_idx : digit_order) {
                for (uint32_t limb_idx : limb_order) {
                    build_for_pair(limb_idx, digit_idx);
                }
            }
        } else {
            for (uint32_t limb_idx : limb_order) {
                for (uint32_t digit_idx : digit_order) {
                    build_for_pair(limb_idx, digit_idx);
                }
            }
        }

        for (uint32_t limb_idx : limb_order) {
            const uint32_t limb_remain =
                plan.problem.limbs - limb_idx * plan.tile_plan.limb_tile;
            const uint32_t limb_now = std::min<uint32_t>(plan.tile_plan.limb_tile, limb_remain);
            const uint64_t out_bytes =
                static_cast<uint64_t>(ct_now) * limb_now * plan.problem.out_limb_bytes;
            const uint64_t moddown_input_bytes = plan.policy.supports_moddown_shortcut
                ? std::max<uint64_t>(1, out_bytes / 2)
                : out_bytes;
            const uint64_t reduction_work =
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(plan.problem.poly_modulus_degree);

            uint64_t reduction_terminal = 0;
            if (plan.policy.supports_partial_reduction_overlap) {
                for (uint32_t digit_idx : digit_order) {
                    std::vector<uint64_t> deps = ConnectSubgraphsWithPolicy(
                        &ctx,
                        ct_idx,
                        limb_idx,
                        digit_idx,
                        plan.policy.innerprod_to_reduction,
                        out_bytes,
                        {inner_terminal[limb_idx][digit_idx]});
                    AddDependencyId(&deps, reduction_terminal);
                    const std::vector<uint64_t> reduce_ids = BuildReductionSubgraph(
                        &ctx,
                        ct_idx,
                        limb_idx,
                        reduction_work,
                        plan.policy,
                        deps);
                    reduction_terminal = reduce_ids.back();
                }
            } else {
                std::vector<uint64_t> deps;
                for (uint32_t digit_idx : digit_order) {
                    const std::vector<uint64_t> conn = ConnectSubgraphsWithPolicy(
                        &ctx,
                        ct_idx,
                        limb_idx,
                        digit_idx,
                        plan.policy.innerprod_to_reduction,
                        out_bytes,
                        {inner_terminal[limb_idx][digit_idx]});
                    for (uint64_t dep : conn) {
                        AddDependencyId(&deps, dep);
                    }
                }
                const std::vector<uint64_t> reduce_ids = BuildReductionSubgraph(
                    &ctx,
                    ct_idx,
                    limb_idx,
                    reduction_work,
                    plan.policy,
                    deps);
                reduction_terminal = reduce_ids.back();
            }

            const std::vector<uint64_t> moddown_deps = ConnectSubgraphsWithPolicy(
                &ctx,
                ct_idx,
                limb_idx,
                0,
                plan.policy.reduction_to_moddown,
                moddown_input_bytes,
                {reduction_terminal});
            const uint64_t moddown_work =
                static_cast<uint64_t>(ct_now)
                * static_cast<uint64_t>(limb_now)
                * static_cast<uint64_t>(plan.problem.polys)
                * static_cast<uint64_t>(plan.problem.poly_modulus_degree);
            const std::vector<uint64_t> moddown_ids = BuildModDownSubgraph(
                &ctx,
                ct_idx,
                limb_idx,
                out_bytes,
                moddown_work,
                plan.policy,
                moddown_deps);

            TileExecutionStep output = MakeStep(
                TileExecutionStepType::OutputBRAMToHBM,
                StageType::Dispatch,
                ct_idx,
                limb_idx,
                0,
                out_bytes,
                out_bytes,
                1,
                IntermediateStorageLevel::BRAM,
                IntermediateStorageLevel::HBM);
            output.bytes = out_bytes;
            output.depends_on = {moddown_ids.back()};
            AppendStep(&ctx, output);
            plan.out_bram_to_hbm_bytes += out_bytes;
        }
    }

    plan.valid = !plan.steps.empty();
    return plan;
}
