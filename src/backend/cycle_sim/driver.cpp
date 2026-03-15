#include "backend/cycle_sim/driver.h"

#include <algorithm>
#include <limits>
#include <vector>

namespace {

struct GroupRuntime {
    uint32_t remaining_dependencies = 0;
    std::vector<uint32_t> dependents;
    uint64_t next_issue_index = 0;
    uint64_t completed_instructions = 0;
    bool started = false;
    bool finished = false;
    uint64_t start_cycle = 0;
    uint64_t finish_cycle = 0;
    uint64_t dependency_wait_cycles = 0;
    uint64_t resource_wait_cycles = 0;
    uint64_t live_bytes_before = 0;
    uint64_t rf_live_bytes_before = 0;
    uint64_t sram_live_bytes_before = 0;
    uint64_t live_bytes_after = 0;
    uint64_t rf_live_bytes_after = 0;
    uint64_t sram_live_bytes_after = 0;
};

bool GroupReadyToIssue(
    const CycleInstructionGroup& group,
    const GroupRuntime& runtime) {

    return !runtime.finished
        && runtime.remaining_dependencies == 0
        && runtime.next_issue_index < group.instructions.size();
}

uint64_t ApplyLiveBytesDelta(
    uint64_t current_live_bytes,
    int64_t delta) {

    if (delta >= 0) {
        return current_live_bytes + static_cast<uint64_t>(delta);
    }

    const uint64_t release_bytes = static_cast<uint64_t>(-delta);
    return (release_bytes >= current_live_bytes)
        ? 0
        : (current_live_bytes - release_bytes);
}

} // namespace

CycleDriver::CycleDriver(const HardwareModel& hardware)
    : hardware_(hardware) {}

CycleSimStats CycleDriver::Run(const CycleProgram& program) const {
    CycleSimStats stats;
    if (program.empty()) {
        return stats;
    }

    std::vector<GroupRuntime> runtime(program.groups.size());
    std::vector<uint32_t> instruction_to_group(
        static_cast<std::size_t>(program.instruction_count),
        std::numeric_limits<uint32_t>::max());

    for (const CycleInstructionGroup& group : program.groups) {
        runtime[group.id].remaining_dependencies =
            static_cast<uint32_t>(group.dependencies.size());
        for (uint32_t dependency : group.dependencies) {
            runtime[dependency].dependents.push_back(group.id);
        }
        for (const CycleInstruction& instruction : group.instructions) {
            if (instruction.id < instruction_to_group.size()) {
                instruction_to_group[static_cast<std::size_t>(instruction.id)] = group.id;
            }
        }
    }

    uint64_t finished_groups = 0;
    uint32_t issue_cursor = 0;
    uint64_t current_live_bytes = 0;
    uint64_t current_rf_live_bytes = 0;
    uint64_t current_sram_live_bytes = 0;
    CycleArch arch(hardware_);

    for (const CycleInstructionGroup& group : program.groups) {
        if (group.instructions.empty()) {
            runtime[group.id].finished = true;
            ++finished_groups;
        }
    }

    uint64_t cycle = 0;
    constexpr uint64_t kMaxSimCycles = 200'000;
    uint64_t spill_count = 0;
    uint64_t reload_count = 0;
    while (finished_groups < program.groups.size() || arch.HasInflight()) {
        if (cycle > kMaxSimCycles) {
            break;
        }
        std::vector<uint64_t> completed_instruction_ids;
        arch.BeginCycle(cycle, &completed_instruction_ids);
        const bool completed_progress = !completed_instruction_ids.empty();
        bool issued_any = false;

        for (uint64_t instruction_id : completed_instruction_ids) {
            if (instruction_id >= instruction_to_group.size()) {
                continue;
            }
            const uint32_t group_id =
                instruction_to_group[static_cast<std::size_t>(instruction_id)];
            if (group_id >= runtime.size()) {
                continue;
            }

            GroupRuntime& group_runtime = runtime[group_id];
            ++group_runtime.completed_instructions;
            if (!group_runtime.finished
                && group_runtime.completed_instructions
                    >= program.groups[group_id].instructions.size()) {
                group_runtime.finished = true;
                group_runtime.finish_cycle = cycle;
                current_live_bytes = ApplyLiveBytesDelta(
                    current_live_bytes,
                    program.groups[group_id].live_bytes_delta_on_complete);
                current_rf_live_bytes = ApplyLiveBytesDelta(
                    current_rf_live_bytes,
                    program.groups[group_id].rf_live_bytes_delta_on_complete);
                current_sram_live_bytes = ApplyLiveBytesDelta(
                    current_sram_live_bytes,
                    program.groups[group_id].sram_live_bytes_delta_on_complete);
                group_runtime.live_bytes_after = current_live_bytes;
                group_runtime.rf_live_bytes_after = current_rf_live_bytes;
                group_runtime.sram_live_bytes_after = current_sram_live_bytes;
                stats.peak_on_chip_live_bytes = std::max<uint64_t>(
                    stats.peak_on_chip_live_bytes,
                    current_live_bytes);
                stats.peak_rf_live_bytes = std::max<uint64_t>(
                    stats.peak_rf_live_bytes,
                    current_rf_live_bytes);
                stats.peak_sram_live_bytes = std::max<uint64_t>(
                    stats.peak_sram_live_bytes,
                    current_sram_live_bytes);
                ++finished_groups;
                for (uint32_t dependent : group_runtime.dependents) {
                    if (runtime[dependent].remaining_dependencies > 0) {
                        --runtime[dependent].remaining_dependencies;
                    }
                }
            }
        }

        bool issued_progress = true;
        while (issued_progress) {
            issued_progress = false;

            std::vector<uint32_t> ready_groups;
            ready_groups.reserve(program.groups.size());
            for (const CycleInstructionGroup& group : program.groups) {
                if (GroupReadyToIssue(group, runtime[group.id])) {
                    ready_groups.push_back(group.id);
                }
            }

            if (ready_groups.empty()) {
                if (finished_groups < program.groups.size()) {
                    ++stats.dependency_stall_cycles;
                }
                break;
            }

            const std::size_t start_offset =
                static_cast<std::size_t>(issue_cursor) % ready_groups.size();
            bool issued_in_round = false;

            for (std::size_t offset = 0; offset < ready_groups.size(); ++offset) {
                const uint32_t group_id =
                    ready_groups[(start_offset + offset) % ready_groups.size()];
                const CycleInstructionGroup& group = program.groups[group_id];
                GroupRuntime& group_runtime = runtime[group_id];
                if (!arch.CanIssue(group.kind, cycle)) {
                    continue;
                }

                if (!group_runtime.started) {
                    group_runtime.started = true;
                    group_runtime.start_cycle = cycle;
                    group_runtime.live_bytes_before = current_live_bytes;
                    group_runtime.rf_live_bytes_before = current_rf_live_bytes;
                    group_runtime.sram_live_bytes_before = current_sram_live_bytes;
                    current_live_bytes = ApplyLiveBytesDelta(
                        current_live_bytes,
                        group.live_bytes_delta_on_issue);
                    current_rf_live_bytes = ApplyLiveBytesDelta(
                        current_rf_live_bytes,
                        group.rf_live_bytes_delta_on_issue);
                    current_sram_live_bytes = ApplyLiveBytesDelta(
                        current_sram_live_bytes,
                        group.sram_live_bytes_delta_on_issue);
                    stats.peak_on_chip_live_bytes = std::max<uint64_t>(
                        stats.peak_on_chip_live_bytes,
                        current_live_bytes);
                    stats.peak_rf_live_bytes = std::max<uint64_t>(
                        stats.peak_rf_live_bytes,
                        current_rf_live_bytes);
                    stats.peak_sram_live_bytes = std::max<uint64_t>(
                        stats.peak_sram_live_bytes,
                        current_sram_live_bytes);
                }

                const CycleInstruction& instruction =
                    group.instructions[static_cast<std::size_t>(group_runtime.next_issue_index)];
                arch.Issue(instruction, cycle);
                ++group_runtime.next_issue_index;
                ++stats.instruction_counts[ToIndex(instruction.kind)];
                stats.instruction_bytes[ToIndex(instruction.kind)] += instruction.bytes;
                issue_cursor = group_id + 1;
                issued_progress = true;
                issued_in_round = true;
                issued_any = true;
            }

            if (!issued_in_round) {
                ++stats.resource_stall_cycles;
                break;
            }
        }

        for (const CycleInstructionGroup& group : program.groups) {
            GroupRuntime& group_runtime = runtime[group.id];
            if (group_runtime.finished) {
                continue;
            }
            if (group_runtime.remaining_dependencies > 0) {
                ++group_runtime.dependency_wait_cycles;
            } else if (group_runtime.next_issue_index < group.instructions.size()
                && !arch.CanIssue(group.kind, cycle)) {
                ++group_runtime.resource_wait_cycles;
            }
        }

        for (const CycleInstructionGroup& group : program.groups) {
            const GroupRuntime& group_runtime = runtime[group.id];
            if (GroupReadyToIssue(group, group_runtime)) {
                arch.RecordStall(group.kind);
            }
        }

        arch.EndCycle();

        if (!completed_progress
            && !issued_any
            && !arch.HasInflight()
            && finished_groups < program.groups.size()) {
            ++stats.dependency_stall_cycles;
            break;
        }

        if (finished_groups >= program.groups.size() && !arch.HasInflight()) {
            break;
        }

        ++cycle;
    }

    for (const CycleInstructionGroup& group : program.groups) {
        const GroupRuntime& group_runtime = runtime[group.id];
        if (!group_runtime.started || !group_runtime.finished) {
            continue;
        }

        CycleGroupTiming timing;
        timing.group_id = group.id;
        timing.kind = group.kind;
        timing.transfer_path = group.transfer_path;
        timing.source_step_type = group.source_step_type;
        timing.stage_type = group.stage_type;
        timing.ct_tile_index = group.ct_tile_index;
        timing.limb_tile_index = group.limb_tile_index;
        timing.digit_tile_index = group.digit_tile_index;
        timing.start_cycle = group_runtime.start_cycle;
        timing.finish_cycle = group_runtime.finish_cycle;
        timing.bytes = group.bytes;
        timing.work_items = group.work_items;
        timing.issued_instructions = group_runtime.next_issue_index;
        timing.completed_instructions = group_runtime.completed_instructions;
        timing.dependency_wait_cycles = group_runtime.dependency_wait_cycles;
        timing.resource_wait_cycles = group_runtime.resource_wait_cycles;
        timing.input_storage = group.input_storage;
        timing.output_storage = group.output_storage;
        timing.fused_with_prev = group.fused_with_prev;
        timing.fused_with_next = group.fused_with_next;
        timing.is_shortcut_path = group.is_shortcut_path;
        timing.live_bytes_before = group_runtime.live_bytes_before;
        timing.live_bytes_after = group_runtime.live_bytes_after;
        timing.rf_live_bytes_before = group_runtime.rf_live_bytes_before;
        timing.rf_live_bytes_after = group_runtime.rf_live_bytes_after;
        timing.sram_live_bytes_before = group_runtime.sram_live_bytes_before;
        timing.sram_live_bytes_after = group_runtime.sram_live_bytes_after;
        stats.group_timings.push_back(timing);

        stats.total_cycles = std::max<uint64_t>(
            stats.total_cycles,
            group_runtime.finish_cycle);
        stats.instruction_cycles[ToIndex(group.kind)] += timing.DurationCycles();
        stats.fine_step_cycles[ToIndex(group.source_step_type)] += timing.DurationCycles();
        stats.fine_step_counts[ToIndex(group.source_step_type)] += 1;

        switch (group.transfer_path) {
        case CycleTransferPath::HostToHBM:
        case CycleTransferPath::HBMToSPM:
            stats.hbm_read_bytes += group.bytes;
            break;
        case CycleTransferPath::SPMToHBM:
            stats.hbm_write_bytes += group.bytes;
            break;
        case CycleTransferPath::None:
            break;
        }

        if (group.source_step_type == TileExecutionStepType::IntermediateBRAMToHBM) {
            stats.spill_bytes += group.bytes;
            ++spill_count;
        }
        if (group.source_step_type == TileExecutionStepType::IntermediateHBMToBRAM) {
            stats.reload_bytes += group.bytes;
            ++reload_count;
        }
    }

    stats.hbm_round_trips = std::min<uint64_t>(spill_count, reload_count);

    stats.component_stats = arch.GetComponentStats();
    return stats;
}
