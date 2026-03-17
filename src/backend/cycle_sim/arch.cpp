#include "backend/cycle_sim/arch.h"

#include <algorithm>
#include <array>
#include <deque>
#include <stdexcept>
#include <utility>

namespace {

struct CompletionEntry {
    uint64_t instruction_id = 0;
    uint64_t completion_cycle = 0;
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
};

HardwareUnitConfig SelectResourceConfig(
    const HardwareModel& hardware,
    ResourceClass rc) {

    switch (rc) {
    case ResourceClass::ComputeArray:
        return hardware.ComputeArrayConfig();
    case ResourceClass::SPU:
        return hardware.SpuConfig();
    case ResourceClass::HBM:
        return hardware.MemoryConfig();
    case ResourceClass::Interconnect:
        return hardware.InterconnectConfig();
    }
    return HardwareUnitConfig{};
}

class ResourceState {
public:
    ResourceState(
        ResourceClass rc,
        const HardwareUnitConfig& config)
        : rc_(rc), config_(config) {

        const uint32_t safe_units = std::max<uint32_t>(1, config_.unit_count);
        next_issue_cycle_.assign(safe_units, 0);
        inflight_by_unit_.resize(safe_units);
    }

    void BeginCycle(
        uint64_t cycle,
        std::vector<CompletionEntry>* completed) {

        issued_this_cycle_ = false;
        stalled_this_cycle_ = false;

        for (auto& queue : inflight_by_unit_) {
            while (!queue.empty() && queue.front().completion_cycle <= cycle) {
                completed->push_back(queue.front());
                queue.pop_front();
            }
        }
    }

    bool CanIssue(uint64_t cycle) const {
        for (std::size_t idx = 0; idx < next_issue_cycle_.size(); ++idx) {
            if (CanIssueOnUnit(idx, cycle)) {
                return true;
            }
        }
        return false;
    }

    void Issue(
        const CycleInstruction& instruction,
        uint64_t cycle) {

        const std::size_t unit_index = SelectIssueUnit(cycle);
        CompletionEntry entry;
        entry.instruction_id = instruction.id;
        entry.kind = instruction.kind;
        entry.completion_cycle =
            cycle + std::max<uint64_t>(1, instruction.latency_cycles);
        inflight_by_unit_[unit_index].push_back(entry);
        next_issue_cycle_[unit_index] =
            cycle + (config_.full_pipeline ? 1 : std::max<uint64_t>(1, instruction.latency_cycles));
        issued_this_cycle_ = true;
        ++total_issued_;
        max_inflight_ = std::max<uint64_t>(max_inflight_, TotalInflight());
        next_unit_cursor_ = static_cast<uint32_t>(
            (unit_index + 1) % next_issue_cycle_.size());
    }

    void RecordStall() {
        stalled_this_cycle_ = true;
    }

    void EndCycle() {
        if (HasInflight() || issued_this_cycle_) {
            ++busy_cycles_;
        }
        if (stalled_this_cycle_) {
            ++stall_cycles_;
        }
    }

    bool HasInflight() const {
        for (const auto& queue : inflight_by_unit_) {
            if (!queue.empty()) {
                return true;
            }
        }
        return false;
    }

    ResourceClass rc() const { return rc_; }
    uint64_t busy_cycles() const { return busy_cycles_; }
    uint64_t stall_cycles() const { return stall_cycles_; }
    uint64_t total_issued() const { return total_issued_; }
    uint64_t max_inflight() const { return max_inflight_; }

private:
    bool CanIssueOnUnit(
        std::size_t unit_index,
        uint64_t cycle) const {

        if (next_issue_cycle_[unit_index] > cycle) {
            return false;
        }

        const auto& queue = inflight_by_unit_[unit_index];
        if (!config_.full_pipeline) {
            return queue.empty();
        }
        return queue.size() < std::max<std::size_t>(1, config_.pipeline_depth);
    }

    std::size_t SelectIssueUnit(uint64_t cycle) const {
        for (std::size_t offset = 0; offset < next_issue_cycle_.size(); ++offset) {
            const std::size_t idx =
                (static_cast<std::size_t>(next_unit_cursor_) + offset)
                % next_issue_cycle_.size();
            if (CanIssueOnUnit(idx, cycle)) {
                return idx;
            }
        }
        throw std::runtime_error("ResourceState issued without capacity");
    }

    uint64_t TotalInflight() const {
        uint64_t total = 0;
        for (const auto& queue : inflight_by_unit_) {
            total += static_cast<uint64_t>(queue.size());
        }
        return total;
    }

private:
    ResourceClass rc_;
    HardwareUnitConfig config_;
    std::vector<uint64_t> next_issue_cycle_;
    std::vector<std::deque<CompletionEntry>> inflight_by_unit_;
    uint32_t next_unit_cursor_ = 0;
    bool issued_this_cycle_ = false;
    bool stalled_this_cycle_ = false;
    uint64_t busy_cycles_ = 0;
    uint64_t stall_cycles_ = 0;
    uint64_t total_issued_ = 0;
    uint64_t max_inflight_ = 0;
};

struct KindStats {
    CycleInstructionKind kind = CycleInstructionKind::LoadHBM;
    uint64_t issued = 0;
    uint64_t completed = 0;
    uint64_t busy_cycles = 0;
    uint64_t stall_cycles = 0;
};

std::array<ResourceState, kResourceClassCount> BuildResources(
    const HardwareModel& hardware) {

    return {
        ResourceState(ResourceClass::ComputeArray, SelectResourceConfig(hardware, ResourceClass::ComputeArray)),
        ResourceState(ResourceClass::SPU, SelectResourceConfig(hardware, ResourceClass::SPU)),
        ResourceState(ResourceClass::HBM, SelectResourceConfig(hardware, ResourceClass::HBM)),
        ResourceState(ResourceClass::Interconnect, SelectResourceConfig(hardware, ResourceClass::Interconnect)),
    };
}

} // namespace

struct CycleArch::Impl {
    explicit Impl(const HardwareModel& hw)
        : hardware(hw),
          resources(BuildResources(hw)) {

        for (std::size_t idx = 0; idx < kCycleInstructionKindCount; ++idx) {
            kind_stats[idx].kind = static_cast<CycleInstructionKind>(idx);
        }
    }

    ResourceState& ForResource(ResourceClass rc) {
        return resources[ToIndex(rc)];
    }

    const ResourceState& ForResource(ResourceClass rc) const {
        return resources[ToIndex(rc)];
    }

    ResourceState& ForKind(CycleInstructionKind kind) {
        return ForResource(ResourceClassOf(kind));
    }

    const ResourceState& ForKind(CycleInstructionKind kind) const {
        return ForResource(ResourceClassOf(kind));
    }

    bool HasInflight() const {
        for (const ResourceState& rs : resources) {
            if (rs.HasInflight()) {
                return true;
            }
        }
        return false;
    }

    std::vector<CycleComponentStats> ExportStats() const {
        std::vector<CycleComponentStats> stats;
        stats.reserve(kCycleInstructionKindCount);
        for (std::size_t idx = 0; idx < kCycleInstructionKindCount; ++idx) {
            const KindStats& ks = kind_stats[idx];
            CycleComponentStats cs;
            cs.kind = ks.kind;
            cs.issued_instructions = ks.issued;
            cs.completed_instructions = ks.completed;
            cs.busy_cycles = ks.busy_cycles;
            cs.stall_cycles = ks.stall_cycles;
            const ResourceState& rs = ForKind(ks.kind);
            cs.max_inflight = rs.max_inflight();
            stats.push_back(cs);
        }
        return stats;
    }

    const HardwareModel& hardware;
    std::array<ResourceState, kResourceClassCount> resources;
    std::array<KindStats, kCycleInstructionKindCount> kind_stats{};
};

CycleArch::CycleArch(const HardwareModel& hardware)
    : impl_(std::make_unique<Impl>(hardware)) {}

CycleArch::~CycleArch() = default;

CycleArch::CycleArch(CycleArch&&) noexcept = default;

CycleArch& CycleArch::operator=(CycleArch&&) noexcept = default;

void CycleArch::BeginCycle(
    uint64_t cycle,
    std::vector<uint64_t>* completed_instruction_ids) {

    std::vector<CompletionEntry> completed;
    for (std::size_t idx = 0; idx < kResourceClassCount; ++idx) {
        impl_->resources[idx].BeginCycle(cycle, &completed);
    }
    for (const CompletionEntry& entry : completed) {
        completed_instruction_ids->push_back(entry.instruction_id);
        ++impl_->kind_stats[ToIndex(entry.kind)].completed;
    }
}

bool CycleArch::CanIssue(
    CycleInstructionKind kind,
    uint64_t cycle) const {

    return impl_->ForKind(kind).CanIssue(cycle);
}

void CycleArch::Issue(
    const CycleInstruction& instruction,
    uint64_t cycle) {

    impl_->ForKind(instruction.kind).Issue(instruction, cycle);
    ++impl_->kind_stats[ToIndex(instruction.kind)].issued;
}

void CycleArch::RecordStall(CycleInstructionKind kind) {
    impl_->ForKind(kind).RecordStall();
    ++impl_->kind_stats[ToIndex(kind)].stall_cycles;
}

void CycleArch::EndCycle() {
    for (std::size_t idx = 0; idx < kResourceClassCount; ++idx) {
        impl_->resources[idx].EndCycle();
    }
    for (std::size_t idx = 0; idx < kCycleInstructionKindCount; ++idx) {
        const CycleInstructionKind kind = static_cast<CycleInstructionKind>(idx);
        const ResourceState& rs = impl_->ForKind(kind);
        if (rs.HasInflight()) {
            ++impl_->kind_stats[idx].busy_cycles;
        }
    }
}

bool CycleArch::HasInflight() const {
    return impl_->HasInflight();
}

std::vector<CycleComponentStats> CycleArch::GetComponentStats() const {
    return impl_->ExportStats();
}
