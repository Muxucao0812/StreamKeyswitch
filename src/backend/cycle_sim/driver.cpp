#include "backend/cycle_sim/driver.h"

#include <algorithm>
#include <limits>
#include <vector>
#include <iostream>

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
    uint64_t live_bytes_after = 0;
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
    // 最终返回的仿真统计结果（总周期、stall、带宽、峰值占用等）。
    CycleSimStats stats;
    // 空程序直接返回全零统计。
    if (program.empty()) {
        return stats;
    }

    // 每个 group 的运行时状态（依赖计数、发射进度、开始/结束周期、等待周期等）。
    std::vector<GroupRuntime> runtime(program.groups.size());
    
    // 指令 ID -> 所属 group ID 的索引表，便于“指令完成事件”反查 group。
    std::vector<uint32_t> instruction_to_group(
        static_cast<std::size_t>(program.instruction_count),
        std::numeric_limits<uint32_t>::max()
    );

    // 初始化依赖关系与反向依赖（dependents），并建立指令到 group 的映射。
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

    // 全局运行状态：
    // - finished_groups: 已完成 group 数
    // - issue_cursor: 轮询发射起点（做简单公平性）
    // - current_live_bytes: 当前片上 BRAM live 占用
    uint64_t finished_groups = 0;
    uint32_t issue_cursor = 0;
    uint64_t current_live_bytes = 0;
    // CycleArch 负责建模各类执行单元与 in-flight 指令行为。
    CycleArch arch(hardware_);

    // 没有指令的 group 视为“已完成”，避免后续循环卡住。
    for (const CycleInstructionGroup& group : program.groups) {
        if (group.instructions.empty()) {
            runtime[group.id].finished = true;
            ++finished_groups;
        }
    }

    // 周期推进主循环。
    uint64_t cycle = 0;
    // 防止异常依赖图导致无限循环，给一个上限保护。
    constexpr uint64_t kMaxSimCycles = 20'000'000'000'000;
    // spill/reload 次数用于最后估算 hbm_round_trips。
    uint64_t spill_count = 0;
    uint64_t reload_count = 0;
    while (finished_groups < program.groups.size() || arch.HasInflight()) {
        if (cycle > kMaxSimCycles) {
            break;
        }
        // 当前周期完成的指令列表（由架构模型填充）。
        std::vector<uint64_t> completed_instruction_ids;
        // 周期起点：让架构推进 in-flight 指令并回收已完成项。
        arch.BeginCycle(cycle, &completed_instruction_ids);
        const bool completed_progress = !completed_instruction_ids.empty();
        // 本周期是否成功发射过新指令（用于死锁检测）。
        bool issued_any = false;

        // 处理“指令完成”事件：更新 group 完成计数，必要时标记 group 完成并释放依赖。
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
            // 当该 group 的所有指令都完成时，触发“group 完成”收尾逻辑。
            if (!group_runtime.finished
                && group_runtime.completed_instructions
                    >= program.groups[group_id].instructions.size()) {
                group_runtime.finished = true;
                group_runtime.finish_cycle = cycle;
                // 应用“完成时内存变化”（通常是释放中间缓冲）。
                current_live_bytes = ApplyLiveBytesDelta(
                    current_live_bytes,
                    program.groups[group_id].live_bytes_delta_on_complete);
                group_runtime.live_bytes_after = current_live_bytes;
                // 更新峰值统计。
                stats.peak_bram_live_bytes = std::max<uint64_t>(
                    stats.peak_bram_live_bytes,
                    current_live_bytes);
                ++finished_groups;
                // 释放其后继 group 的依赖计数。
                for (uint32_t dependent : group_runtime.dependents) {
                    if (runtime[dependent].remaining_dependencies > 0) {
                        --runtime[dependent].remaining_dependencies;
                    }
                }
            }
        }

        // 尝试在当前周期尽可能多地发射指令：
        // 每轮收集 ready group -> 按轮询顺序尝试发射 -> 发不动就记资源 stall。
        bool issued_progress = true;
        while (issued_progress) {
            issued_progress = false;

            std::vector<uint32_t> ready_groups;
            ready_groups.reserve(program.groups.size());
            // ready 条件：未完成、依赖清零、仍有未发射指令。
            for (const CycleInstructionGroup& group : program.groups) {
                if (GroupReadyToIssue(group, runtime[group.id])) {
                    ready_groups.push_back(group.id);
                }
            }

            // 没有 ready group 但仍有未完成 group，说明在等依赖（dependency stall）。
            if (ready_groups.empty()) {
                if (finished_groups < program.groups.size()) {
                    ++stats.dependency_stall_cycles;
                }
                break;
            }

            // 轮询起点，避免总是从固定 group 开始导致饥饿。
            const std::size_t start_offset =
                static_cast<std::size_t>(issue_cursor) % ready_groups.size();
            bool issued_in_round = false;

            for (std::size_t offset = 0; offset < ready_groups.size(); ++offset) {
                const uint32_t group_id =
                    ready_groups[(start_offset + offset) % ready_groups.size()];
                const CycleInstructionGroup& group = program.groups[group_id];
                GroupRuntime& group_runtime = runtime[group_id];
                // 功能单元忙则跳过该 group，尝试其他 ready group。
                if (!arch.CanIssue(group.kind, cycle)) {
                    continue;
                }

                // group 第一次发射时，记录“发射前快照”，并应用“发射时内存变化”。
                if (!group_runtime.started) {
                    group_runtime.started = true;
                    group_runtime.start_cycle = cycle;
                    group_runtime.live_bytes_before = current_live_bytes;
                    current_live_bytes = ApplyLiveBytesDelta(
                        current_live_bytes,
                        group.live_bytes_delta_on_issue);
                    stats.peak_bram_live_bytes = std::max<uint64_t>(
                        stats.peak_bram_live_bytes,
                        current_live_bytes);
                }

                // 发射该 group 当前待发射的下一条指令。
                const CycleInstruction& instruction =
                    group.instructions[static_cast<std::size_t>(group_runtime.next_issue_index)];
                arch.Issue(instruction, cycle);
                ++group_runtime.next_issue_index;
                // 指令级统计：按 kind 记录条数与字节。
                ++stats.instruction_counts[ToIndex(instruction.kind)];
                stats.instruction_bytes[ToIndex(instruction.kind)] += instruction.bytes;
                // 下一轮从当前 group 后面开始，形成近似 round-robin。
                issue_cursor = group_id + 1;
                issued_progress = true;
                issued_in_round = true;
                issued_any = true;
            }

            // 本轮有 ready group 但一条都发不出去，判为资源冲突 stall。
            if (!issued_in_round) {
                ++stats.resource_stall_cycles;
                break;
            }
        }

        // 逐 group 累计“等待来源”统计：
        // - 依赖未满足：dependency_wait_cycles
        // - 依赖满足但发不出：resource_wait_cycles
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

        // 对 ready 但因资源不可发射的 group 记录组件级 stall。
        for (const CycleInstructionGroup& group : program.groups) {
            const GroupRuntime& group_runtime = runtime[group.id];
            if (GroupReadyToIssue(group, group_runtime)) {
                arch.RecordStall(group.kind);
            }
        }

        // 周期结束，让架构内部完成本周期状态提交。
        arch.EndCycle();

        // 死锁/停滞保护：
        // 本周期既没有完成也没有发射，且无 inflight，但还有未完成 group -> 强制退出。
        if (!completed_progress
            && !issued_any
            && !arch.HasInflight()
            && finished_groups < program.groups.size()) {
            ++stats.dependency_stall_cycles;
            break;
        }

        // 全部 group 完成且没有 inflight 指令，正常退出。
        if (finished_groups >= program.groups.size() && !arch.HasInflight()) {
            break;
        }

        // 进入下一周期。
        ++cycle;
    }

    // 将运行期 runtime 信息回填为可导出的 group 级 timing 记录，并汇总统计。
    for (const CycleInstructionGroup& group : program.groups) {
        const GroupRuntime& group_runtime = runtime[group.id];
        if (!group_runtime.started || !group_runtime.finished) {
            continue;
        }

        // 组织单 group 的时序与元数据记录。
        CycleGroupTiming timing;
        timing.group_id = group.id;
        timing.kind = group.kind;
        timing.transfer_path = group.transfer_path;
        timing.type = group.type;
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
        stats.group_timings.push_back(timing);

        const TileExecutionStepType fine_step_type = FineStepTypeOf(group.type);

        // 汇总总周期与分项周期统计。
        stats.total_cycles = std::max<uint64_t>(
            stats.total_cycles,
            group_runtime.finish_cycle);
        stats.instruction_cycles[ToIndex(group.kind)] += timing.DurationCycles();
        stats.fine_step_cycles[ToIndex(fine_step_type)] += timing.DurationCycles();
        stats.fine_step_counts[ToIndex(fine_step_type)] += 1;

        // 按传输路径累计 HBM 读写字节。
        switch (group.transfer_path) {
        case CycleTransferPath::HostToHBM:
        case CycleTransferPath::HBMToSPM:
            stats.hbm_read_bytes += group.bytes;
            break;
        case CycleTransferPath::SPMToHBM:
            stats.hbm_write_bytes += group.bytes;
            break;
        case CycleTransferPath::HBMToHBM:
            stats.hbm_read_bytes += group.bytes;
            stats.hbm_write_bytes += group.bytes;
            break;
        case CycleTransferPath::None:
            break;
        }

        // 统计 spill/reload 字节与次数（用于 round-trip 估计）。
        if (fine_step_type == TileExecutionStepType::IntermediateBRAMToHBM) {
            stats.spill_bytes += group.bytes;
            ++spill_count;
        }
        if (fine_step_type == TileExecutionStepType::IntermediateHBMToBRAM) {
            stats.reload_bytes += group.bytes;
            ++reload_count;
        }
    }

    // round-trip 以可配对的 spill/reload 次数取最小值估算。
    stats.hbm_round_trips = std::min<uint64_t>(spill_count, reload_count);

    // 回填组件级统计（各功能单元的 issued/completed/busy/stall/max_inflight）。
    stats.component_stats = arch.GetComponentStats();
    return stats;
}
