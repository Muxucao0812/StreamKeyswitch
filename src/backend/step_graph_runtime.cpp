#include "backend/step_graph_runtime.h"

#include <algorithm>
#include <limits>
#include <unordered_map>

namespace {

template <typename T>
void SaturatingAdd(T* dst, T value) {
    const __uint128_t sum =
        static_cast<__uint128_t>(*dst) + static_cast<__uint128_t>(value);
    const __uint128_t limit =
        static_cast<__uint128_t>(std::numeric_limits<T>::max());
    *dst = (sum > limit) ? std::numeric_limits<T>::max() : static_cast<T>(sum);
}

IntermediateStorageLevel CanonicalizeStorage(IntermediateStorageLevel storage) {
    return (storage == IntermediateStorageLevel::HBM)
        ? IntermediateStorageLevel::HBM
        : IntermediateStorageLevel::BRAM;
}

void UpdatePeaks(RuntimeState* state) {
    state->peak_hbm_bytes = std::max(state->peak_hbm_bytes, state->live_hbm_bytes);
    state->peak_bram_bytes = std::max(state->peak_bram_bytes, state->live_bram_bytes);
}

void ReleaseLiveBytes(
    IntermediateStorageLevel storage,
    uint64_t bytes,
    RuntimeState* state) {

    uint64_t* live_bytes = nullptr;
    switch (CanonicalizeStorage(storage)) {
    case IntermediateStorageLevel::BRAM:
        live_bytes = &state->live_bram_bytes;
        break;
    case IntermediateStorageLevel::HBM:
        live_bytes = &state->live_hbm_bytes;
        break;
    }

    if (live_bytes == nullptr) {
        return;
    }
    *live_bytes = (bytes >= *live_bytes) ? 0 : (*live_bytes - bytes);
}

uint64_t AccountRead(
    const HardwareModel& hardware,
    IntermediateStorageLevel storage,
    uint64_t bytes,
    RuntimeState* state) {

    if (bytes == 0) {
        return 0;
    }

    switch (CanonicalizeStorage(storage)) {
    case IntermediateStorageLevel::BRAM:
        SaturatingAdd(&state->bram_read_bytes, bytes);
        return hardware.EstimateBRAMReadCycles(bytes);
    case IntermediateStorageLevel::HBM:
        SaturatingAdd(&state->hbm_read_bytes, bytes);
        return hardware.EstimateTransferCycles(HardwareTransferPath::HBMToSPM, bytes);
    }

    return 0;
}

uint64_t AccountWrite(
    const HardwareModel& hardware,
    IntermediateStorageLevel storage,
    uint64_t bytes,
    bool track_live_bytes,
    RuntimeState* state) {

    if (bytes == 0) {
        return 0;
    }

    uint64_t cycles = 0;
    switch (CanonicalizeStorage(storage)) {
    case IntermediateStorageLevel::BRAM:
        SaturatingAdd(&state->bram_write_bytes, bytes);
        cycles = hardware.EstimateBRAMWriteCycles(bytes);
        if (track_live_bytes) {
            SaturatingAdd(&state->live_bram_bytes, bytes);
        }
        break;
    case IntermediateStorageLevel::HBM:
        SaturatingAdd(&state->hbm_write_bytes, bytes);
        cycles = hardware.EstimateTransferCycles(HardwareTransferPath::SPMToHBM, bytes);
        if (track_live_bytes) {
            SaturatingAdd(&state->live_hbm_bytes, bytes);
        }
        break;
    }

    if (track_live_bytes) {
        UpdatePeaks(state);
    }
    return cycles;
}

uint64_t AccountHostToHBMWrite(
    const HardwareModel& hardware,
    uint64_t bytes,
    bool track_live_bytes,
    RuntimeState* state) {

    if (bytes == 0) {
        return 0;
    }

    SaturatingAdd(&state->hbm_write_bytes, bytes);
    if (track_live_bytes) {
        SaturatingAdd(&state->live_hbm_bytes, bytes);
        UpdatePeaks(state);
    }
    return hardware.EstimateTransferCycles(HardwareTransferPath::HostToHBM, bytes);
}

uint32_t ConsumerCount(
    const RuntimePlan& plan,
    uint64_t step_id) {

    uint32_t count = 0;
    for (const TileExecutionStep& step : plan.steps) {
        count += static_cast<uint32_t>(std::count(
            step.depends_on.begin(),
            step.depends_on.end(),
            step_id));
    }
    return count;
}

std::vector<const TileExecutionStep*> ConsumersOf(
    const RuntimePlan& plan,
    uint64_t step_id) {

    std::vector<const TileExecutionStep*> consumers;
    for (const TileExecutionStep& step : plan.steps) {
        if (std::find(step.depends_on.begin(), step.depends_on.end(), step_id)
            != step.depends_on.end()) {
            consumers.push_back(&step);
        }
    }
    return consumers;
}

bool IsComputeStepType(TileExecutionStepType type) {
    switch (type) {
    case TileExecutionStepType::ModUpInttTile:
    case TileExecutionStepType::ModDownInttTile:
    case TileExecutionStepType::ModUpBConvTile:
    case TileExecutionStepType::ModDownBConvTile:
    case TileExecutionStepType::ModUpNttTile:
    case TileExecutionStepType::ModDownNttTile:
    case TileExecutionStepType::KSInnerProdTile:
    case TileExecutionStepType::CrossDigitReduceTile:
    case TileExecutionStepType::FinalSubtractTile:
    case TileExecutionStepType::DecomposeTile:
    case TileExecutionStepType::NttTile:
    case TileExecutionStepType::InttTile:
    case TileExecutionStepType::BasisConvertTile:
    case TileExecutionStepType::AccumulateSubtractTile:
        return true;
    default:
        return false;
    }
}

uint64_t StepDurationCycles(const StepRuntimeRecord& record) {
    return std::max(
        record.compute_cycles,
        std::max(record.bram_transfer_cycles, record.hbm_transfer_cycles));
}

uint64_t BaseCyclesIfActive(uint64_t base_cycles, uint64_t work_items) {
    return (work_items == 0) ? 0 : base_cycles;
}

IntermediateStorageLevel ResolveOutputStorage(
    const RuntimePlan& plan,
    const TileExecutionStep& step) {

    if (!step.materialize_output) {
        return step.output_storage;
    }

    if (IsComputeStepType(step.type) && step.output_storage == IntermediateStorageLevel::HBM) {
        const std::vector<const TileExecutionStep*> consumers = ConsumersOf(plan, step.step_id);
        for (const TileExecutionStep* consumer : consumers) {
            if (consumer == nullptr) {
                continue;
            }
            if (consumer->type == TileExecutionStepType::IntermediateBRAMToHBM) {
                return consumer->input_storage;
            }
        }
        return IntermediateStorageLevel::BRAM;
    }

    return step.output_storage;
}

void CreateOutputHandle(
    const HardwareModel& hardware,
    const RuntimePlan& plan,
    const TileExecutionStep& step,
    StepRuntimeRecord* record,
    RuntimeState* state) {

    const uint32_t consumers = ConsumerCount(plan, step.step_id);
    const bool persistent = step.key_persistent;
    const bool keep_handle = (consumers > 0) || persistent;
    const IntermediateStorageLevel storage = CanonicalizeStorage(
        ResolveOutputStorage(plan, step));

    if (!step.materialize_output) {
        if (!keep_handle || step.output_bytes == 0) {
            return;
        }
        TensorHandle handle;
        handle.id = step.step_id;
        handle.producer_step_id = step.step_id;
        handle.bytes = step.output_bytes;
        handle.storage = storage;
        handle.remaining_uses = consumers;
        handle.alive = true;
        handle.materialized = false;
        handle.persistent = persistent;
        state->tensors[handle.id] = handle;
        return;
    }

    record->transfer_bytes += step.output_bytes;
    const uint64_t write_cycles = AccountWrite(
        hardware,
        storage,
        step.output_bytes,
        keep_handle,
        state);
    record->transfer_cycles += write_cycles;
    switch (storage) {
    case IntermediateStorageLevel::BRAM:
        record->bram_transfer_cycles += write_cycles;
        break;
    case IntermediateStorageLevel::HBM:
        record->hbm_transfer_cycles += write_cycles;
        break;
    }

    if (!keep_handle || step.output_bytes == 0) {
        return;
    }

    TensorHandle handle;
    handle.id = step.step_id;
    handle.producer_step_id = step.step_id;
    handle.bytes = step.output_bytes;
    handle.storage = storage;
    handle.remaining_uses = consumers;
    handle.alive = true;
    handle.materialized = true;
    handle.persistent = persistent;
    state->tensors[handle.id] = handle;
}

std::vector<const TensorHandle*> CollectInputHandles(
    const TileExecutionStep& step,
    RuntimeState* state) {

    std::vector<const TensorHandle*> inputs;
    inputs.reserve(step.depends_on.size());
    for (const uint64_t dep_step_id : step.depends_on) {
        const auto it = state->tensors.find(dep_step_id);
        if (it == state->tensors.end() || !it->second.alive) {
            state->valid = false;
            return {};
        }
        inputs.push_back(&it->second);
    }
    return inputs;
}

void ReleaseDeadInputs(
    const TileExecutionStep& step,
    RuntimeState* state) {

    for (const uint64_t dep_step_id : step.depends_on) {
        auto it = state->tensors.find(dep_step_id);
        if (it == state->tensors.end()) {
            continue;
        }
        TensorHandle& handle = it->second;
        if (handle.remaining_uses > 0) {
            --handle.remaining_uses;
        }
        if (handle.remaining_uses == 0 && !handle.persistent) {
            if (handle.materialized) {
                ReleaseLiveBytes(handle.storage, handle.bytes, state);
            }
            state->tensors.erase(it);
        }
    }
}

void FinalizeRecord(
    StepRuntimeRecord* record,
    RuntimeState* state) {

    record->total_cycles = StepDurationCycles(*record);
    record->live_bram_bytes_after = state->live_bram_bytes;
    record->live_hbm_bytes_after = state->live_hbm_bytes;

    SaturatingAdd(&state->compute_cycles, record->compute_cycles);
    SaturatingAdd(&state->transfer_cycles, record->transfer_cycles);
    SaturatingAdd(
        &state->fine_step_cycles[ToIndex(record->step_type)],
        record->total_cycles);
    state->step_records.push_back(*record);
}

uint64_t EstimatePipelinedTotalCycles(
    const RuntimePlan& plan,
    const RuntimeState& state) {

    std::unordered_map<uint64_t, const StepRuntimeRecord*> records_by_step_id;
    records_by_step_id.reserve(state.step_records.size());
    for (const StepRuntimeRecord& record : state.step_records) {
        records_by_step_id.emplace(record.step_id, &record);
    }

    std::unordered_map<uint64_t, uint64_t> done_cycles_by_step_id;
    done_cycles_by_step_id.reserve(state.step_records.size());

    uint64_t bram_ready_cycle = 0;
    uint64_t hbm_ready_cycle = 0;
    uint64_t compute_ready_cycle = 0;
    uint64_t total_cycles = 0;

    for (const TileExecutionStep& step : plan.steps) {
        const auto record_it = records_by_step_id.find(step.step_id);
        if (record_it == records_by_step_id.end()) {
            continue;
        }

        const StepRuntimeRecord& record = *record_it->second;
        uint64_t deps_ready_cycle = 0;
        for (const uint64_t dep_step_id : step.depends_on) {
            const auto done_it = done_cycles_by_step_id.find(dep_step_id);
            if (done_it == done_cycles_by_step_id.end()) {
                continue;
            }
            deps_ready_cycle = std::max(deps_ready_cycle, done_it->second);
        }

        uint64_t start_cycle = deps_ready_cycle;
        if (record.bram_transfer_cycles > 0) {
            start_cycle = std::max(start_cycle, bram_ready_cycle);
        }
        if (record.hbm_transfer_cycles > 0) {
            start_cycle = std::max(start_cycle, hbm_ready_cycle);
        }
        if (record.compute_cycles > 0) {
            start_cycle = std::max(start_cycle, compute_ready_cycle);
        }

        if (record.bram_transfer_cycles > 0) {
            bram_ready_cycle = start_cycle + record.bram_transfer_cycles;
        }
        if (record.hbm_transfer_cycles > 0) {
            hbm_ready_cycle = start_cycle + record.hbm_transfer_cycles;
        }
        if (record.compute_cycles > 0) {
            compute_ready_cycle = start_cycle + record.compute_cycles;
        }

        const uint64_t done_cycle = start_cycle + StepDurationCycles(record);
        done_cycles_by_step_id.emplace(step.step_id, done_cycle);
        total_cycles = std::max(total_cycles, done_cycle);
    }

    return total_cycles;
}

void ExecTransferOrLoad(
    const HardwareModel& hardware,
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    const StepGraphRuntimeExecutor* executor,
    StepRuntimeRecord* record,
    RuntimeState* state) {

    if (inputs.empty()) {
        switch (step.type) {
        case TileExecutionStepType::KeyLoadHostToHBM:
            record->transfer_bytes += step.output_bytes;
            record->hbm_transfer_cycles += AccountHostToHBMWrite(
                hardware,
                step.output_bytes,
                ConsumerCount(plan, step.step_id) > 0,
                state);
            record->transfer_cycles += record->hbm_transfer_cycles;
            if (ConsumerCount(plan, step.step_id) > 0) {
                TensorHandle handle;
                handle.id = step.step_id;
                handle.producer_step_id = step.step_id;
                handle.bytes = step.output_bytes;
                handle.storage = IntermediateStorageLevel::HBM;
                handle.remaining_uses = ConsumerCount(plan, step.step_id);
                handle.alive = true;
                handle.materialized = true;
                state->tensors[handle.id] = handle;
            }
            return;
        case TileExecutionStepType::InputHBMToBRAM:
        case TileExecutionStepType::KeyLoadHBMToBRAM:
        case TileExecutionStepType::KeyHBMToBRAM:
        case TileExecutionStepType::IntermediateHBMToBRAM:
            record->transfer_bytes += step.input_bytes;
            record->hbm_transfer_cycles += hardware.EstimateTransferCycles(
                HardwareTransferPath::HBMToSPM,
                step.input_bytes);
            record->transfer_cycles += record->hbm_transfer_cycles;
            SaturatingAdd(&state->hbm_read_bytes, step.input_bytes);
            CreateOutputHandle(hardware, plan, step, record, state);
            if (step.type == TileExecutionStepType::IntermediateHBMToBRAM) {
                record->is_reload = true;
                ++state->reload_count;
                SaturatingAdd(&state->reload_bytes, step.output_bytes);
            }
            return;
        default:
            break;
        }
    }

    for (const TensorHandle* input : inputs) {
        if (input == nullptr) {
            state->valid = false;
            return;
        }
        const MoveResult move = executor->MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    CreateOutputHandle(hardware, plan, step, record, state);
    if (step.type == TileExecutionStepType::IntermediateBRAMToHBM) {
        record->is_spill = true;
        ++state->spill_count;
        SaturatingAdd(&state->spill_bytes, step.output_bytes);
    } else if (step.type == TileExecutionStepType::IntermediateHBMToBRAM) {
        record->is_reload = true;
        ++state->reload_count;
        SaturatingAdd(&state->reload_bytes, step.output_bytes);
    }
}

} // namespace

StepGraphRuntimeExecutor::StepGraphRuntimeExecutor(const HardwareModel& hardware)
    : hardware_(hardware) {}

RuntimeState StepGraphRuntimeExecutor::ExecuteStepGraph(
    const RuntimePlan& plan) const {

    RuntimeState state;
    state.valid = plan.valid;
    if (!plan.valid) {
        return state;
    }

    state.step_records.reserve(plan.steps.size());
    for (const TileExecutionStep& step : plan.steps) {
        StepRuntimeRecord record;
        record.step_id = step.step_id;
        record.step_type = step.type;
        record.stage_type = step.stage_type;
        record.input_bytes = step.input_bytes;
        record.output_bytes = step.output_bytes;
        record.live_bram_bytes_before = state.live_bram_bytes;
        record.live_hbm_bytes_before = state.live_hbm_bytes;

        const std::vector<const TensorHandle*> inputs = CollectInputHandles(step, &state);
        if (!state.valid) {
            break;
        }

        switch (step.type) {
        case TileExecutionStepType::ModUpInttTile:
        case TileExecutionStepType::ModDownInttTile:
        case TileExecutionStepType::InttTile:
            ExecINTT(step, inputs, plan, &record, &state);
            break;
        case TileExecutionStepType::ModUpBConvTile:
        case TileExecutionStepType::ModDownBConvTile:
        case TileExecutionStepType::BasisConvertTile:
            ExecBConv(step, inputs, plan, &record, &state);
            break;
        case TileExecutionStepType::ModUpNttTile:
        case TileExecutionStepType::ModDownNttTile:
        case TileExecutionStepType::NttTile:
            ExecNTT(step, inputs, plan, &record, &state);
            break;
        case TileExecutionStepType::KSInnerProdTile:
            ExecInnerProd(step, inputs, plan, &record, &state);
            break;
        case TileExecutionStepType::CrossDigitReduceTile:
        case TileExecutionStepType::AccumulateSubtractTile:
            ExecReduce(step, inputs, plan, &record, &state);
            break;
        case TileExecutionStepType::FinalSubtractTile:
            ExecSubtract(step, inputs, plan, &record, &state);
            break;
        default:
            ExecTransferOrLoad(
                hardware_,
                step,
                inputs,
                plan,
                this,
                &record,
                &state);
            break;
        }

        if (!state.valid) {
            break;
        }

        ReleaseDeadInputs(step, &state);
        FinalizeRecord(&record, &state);
    }

    if (state.valid) {
        state.total_cycles = EstimatePipelinedTotalCycles(plan, state);
    }

    return state;
}

RuntimeState StepGraphRuntimeExecutor::ExecuteStepGraph(
    const KeySwitchExecution& execution) const {

    RuntimePlan plan;
    plan.valid = execution.valid;
    plan.problem = execution.problem;
    plan.policy = execution.policy;
    plan.tile_plan = execution.tile_plan;
    plan.steps = execution.steps;
    plan.tile_count = execution.tile_count;
    plan.key_resident_hit = execution.key_resident_hit;
    plan.key_persistent_bram = execution.key_persistent_bram;
    plan.working_set_bytes = execution.working_set_bytes;
    plan.key_host_to_hbm_bytes = execution.key_host_to_hbm_bytes;
    plan.key_hbm_to_bram_bytes = execution.key_hbm_to_bram_bytes;
    plan.ct_hbm_to_bram_bytes = execution.ct_hbm_to_bram_bytes;
    plan.out_bram_to_hbm_bytes = execution.out_bram_to_hbm_bytes;
    return ExecuteStepGraph(plan);
}

MoveResult StepGraphRuntimeExecutor::MoveOrForward(
    const TileExecutionStep& /*step*/,
    const TensorHandle& input,
    RuntimeState* state) const {

    MoveResult result;
    result.bytes = input.bytes;
    if (!input.materialized) {
        result.direct_forward = true;
        result.bram_cycles = hardware_.EstimateDirectForwardCycles(input.bytes);
        result.cycles = result.bram_cycles;
        ++state->direct_forward_count;
        SaturatingAdd(&state->direct_forward_bytes, input.bytes);
        return result;
    }

    result.cycles = AccountRead(
        hardware_,
        input.storage,
        input.bytes,
        state);
    switch (CanonicalizeStorage(input.storage)) {
    case IntermediateStorageLevel::BRAM:
        result.bram_cycles = result.cycles;
        break;
    case IntermediateStorageLevel::HBM:
        result.hbm_cycles = result.cycles;
        break;
    }
    return result;
}

void StepGraphRuntimeExecutor::ExecINTT(
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    StepRuntimeRecord* record,
    RuntimeState* state) const {

    for (const TensorHandle* input : inputs) {
        const MoveResult move = MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    record->compute_cycles = BaseCyclesIfActive(
        hardware_.EstimateInttCycles(plan.problem),
        step.work_items);
    CreateOutputHandle(hardware_, plan, step, record, state);
}

void StepGraphRuntimeExecutor::ExecBConv(
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    StepRuntimeRecord* record,
    RuntimeState* state) const {

    for (const TensorHandle* input : inputs) {
        const MoveResult move = MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    record->compute_cycles = BaseCyclesIfActive(
        hardware_.EstimateBconvCycles(plan.problem),
        step.work_items);
    CreateOutputHandle(hardware_, plan, step, record, state);
}

void StepGraphRuntimeExecutor::ExecNTT(
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    StepRuntimeRecord* record,
    RuntimeState* state) const {

    for (const TensorHandle* input : inputs) {
        const MoveResult move = MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    record->compute_cycles = BaseCyclesIfActive(
        hardware_.EstimateNttCycles(plan.problem),
        step.work_items);
    CreateOutputHandle(hardware_, plan, step, record, state);
}

void StepGraphRuntimeExecutor::ExecInnerProd(
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    StepRuntimeRecord* record,
    RuntimeState* state) const {

    for (const TensorHandle* input : inputs) {
        const MoveResult move = MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    const uint64_t mul_cycles = BaseCyclesIfActive(
        hardware_.EstimateEweMulCycles(plan.problem),
        step.work_items);
    const uint64_t add_cycles = (plan.problem.polys > 1)
        ? BaseCyclesIfActive(
            hardware_.EstimateEweAddCycles(plan.problem),
            step.work_items)
        : 0;
    record->compute_cycles = mul_cycles + add_cycles;
    CreateOutputHandle(hardware_, plan, step, record, state);
}

void StepGraphRuntimeExecutor::ExecReduce(
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    StepRuntimeRecord* record,
    RuntimeState* state) const {

    for (const TensorHandle* input : inputs) {
        const MoveResult move = MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    record->compute_cycles = BaseCyclesIfActive(
        hardware_.EstimateEweAddCycles(plan.problem),
        step.work_items);
    CreateOutputHandle(hardware_, plan, step, record, state);
}

void StepGraphRuntimeExecutor::ExecSubtract(
    const TileExecutionStep& step,
    const std::vector<const TensorHandle*>& inputs,
    const RuntimePlan& plan,
    StepRuntimeRecord* record,
    RuntimeState* state) const {

    for (const TensorHandle* input : inputs) {
        const MoveResult move = MoveOrForward(step, *input, state);
        record->transfer_bytes += move.bytes;
        record->transfer_cycles += move.cycles;
        record->bram_transfer_cycles += move.bram_cycles;
        record->hbm_transfer_cycles += move.hbm_cycles;
        record->used_direct_forward = record->used_direct_forward || move.direct_forward;
    }

    record->compute_cycles = BaseCyclesIfActive(
        hardware_.EstimateEweSubCycles(plan.problem),
        step.work_items);
    CreateOutputHandle(hardware_, plan, step, record, state);
}
