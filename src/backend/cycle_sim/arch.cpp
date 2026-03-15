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
};

HardwareUnitConfig SelectConfig(
    const HardwareModel& hardware,
    CycleInstructionKind kind) {

    switch (kind) {
    case CycleInstructionKind::LoadHBM:
    case CycleInstructionKind::StoreHBM:
        return hardware.MemoryConfig();
    case CycleInstructionKind::Decompose:
        return hardware.DecomposeConfig();
    case CycleInstructionKind::NTT:
    case CycleInstructionKind::INTT:
        return hardware.NttConfig();
    case CycleInstructionKind::EweMul:
        return hardware.EweMulConfig();
    case CycleInstructionKind::EweAdd:
        return hardware.EweAddConfig();
    case CycleInstructionKind::EweSub:
        return hardware.EweSubConfig();
    case CycleInstructionKind::BConv:
        return hardware.BconvConfig();
    }

    return HardwareUnitConfig{};
}

class ComponentState {
public:
    ComponentState(
        CycleInstructionKind kind,
        const HardwareUnitConfig& config)
        : config_(config) {

        stats_.kind = kind;
        const uint32_t safe_units = std::max<uint32_t>(1, config_.unit_count);
        next_issue_cycle_.assign(safe_units, 0);
        inflight_by_unit_.resize(safe_units);
    }

    void BeginCycle(
        uint64_t cycle,
        std::vector<uint64_t>* completed_instruction_ids) {

        issued_this_cycle_ = false;
        stalled_this_cycle_ = false;

        for (auto& queue : inflight_by_unit_) {
            while (!queue.empty() && queue.front().completion_cycle <= cycle) {
                completed_instruction_ids->push_back(queue.front().instruction_id);
                queue.pop_front();
                ++stats_.completed_instructions;
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
        entry.completion_cycle =
            cycle + std::max<uint64_t>(1, instruction.latency_cycles);
        inflight_by_unit_[unit_index].push_back(entry);
        next_issue_cycle_[unit_index] =
            cycle + (config_.full_pipeline ? 1 : std::max<uint64_t>(1, instruction.latency_cycles));
        issued_this_cycle_ = true;
        ++stats_.issued_instructions;
        stats_.max_inflight = std::max<uint64_t>(
            stats_.max_inflight,
            TotalInflight());
        next_unit_cursor_ = static_cast<uint32_t>(
            (unit_index + 1) % next_issue_cycle_.size());
    }

    void RecordStall() {
        stalled_this_cycle_ = true;
    }

    void EndCycle() {
        if (HasInflight() || issued_this_cycle_) {
            ++stats_.busy_cycles;
        }
        if (stalled_this_cycle_) {
            ++stats_.stall_cycles;
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

    const CycleComponentStats& stats() const {
        return stats_;
    }

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
        throw std::runtime_error("CycleArch issued without capacity");
    }

    uint64_t TotalInflight() const {
        uint64_t total = 0;
        for (const auto& queue : inflight_by_unit_) {
            total += static_cast<uint64_t>(queue.size());
        }
        return total;
    }

private:
    HardwareUnitConfig config_;
    CycleComponentStats stats_;
    std::vector<uint64_t> next_issue_cycle_;
    std::vector<std::deque<CompletionEntry>> inflight_by_unit_;
    uint32_t next_unit_cursor_ = 0;
    bool issued_this_cycle_ = false;
    bool stalled_this_cycle_ = false;
};

std::array<ComponentState, kCycleInstructionKindCount> BuildComponents(
    const HardwareModel& hardware) {

    return {
        ComponentState(CycleInstructionKind::LoadHBM, SelectConfig(hardware, CycleInstructionKind::LoadHBM)),
        ComponentState(CycleInstructionKind::StoreHBM, SelectConfig(hardware, CycleInstructionKind::StoreHBM)),
        ComponentState(CycleInstructionKind::Decompose, SelectConfig(hardware, CycleInstructionKind::Decompose)),
        ComponentState(CycleInstructionKind::NTT, SelectConfig(hardware, CycleInstructionKind::NTT)),
        ComponentState(CycleInstructionKind::INTT, SelectConfig(hardware, CycleInstructionKind::INTT)),
        ComponentState(CycleInstructionKind::EweMul, SelectConfig(hardware, CycleInstructionKind::EweMul)),
        ComponentState(CycleInstructionKind::EweAdd, SelectConfig(hardware, CycleInstructionKind::EweAdd)),
        ComponentState(CycleInstructionKind::EweSub, SelectConfig(hardware, CycleInstructionKind::EweSub)),
        ComponentState(CycleInstructionKind::BConv, SelectConfig(hardware, CycleInstructionKind::BConv)),
    };
}

} // namespace

struct CycleArch::Impl {
    explicit Impl(const HardwareModel& hw)
        : hardware(hw),
          components(BuildComponents(hw)) {}

    ComponentState& ForKind(CycleInstructionKind kind) {
        return components[ToIndex(kind)];
    }

    const ComponentState& ForKind(CycleInstructionKind kind) const {
        return components[ToIndex(kind)];
    }

    bool HasInflight() const {
        for (const ComponentState& component : components) {
            if (component.HasInflight()) {
                return true;
            }
        }
        return false;
    }

    std::vector<CycleComponentStats> ExportStats() const {
        std::vector<CycleComponentStats> stats;
        stats.reserve(components.size());
        for (const ComponentState& component : components) {
            stats.push_back(component.stats());
        }
        return stats;
    }

    const HardwareModel& hardware;
    std::array<ComponentState, kCycleInstructionKindCount> components;
};

CycleArch::CycleArch(const HardwareModel& hardware)
    : impl_(std::make_unique<Impl>(hardware)) {}

CycleArch::~CycleArch() = default;

CycleArch::CycleArch(CycleArch&&) noexcept = default;

CycleArch& CycleArch::operator=(CycleArch&&) noexcept = default;

void CycleArch::BeginCycle(
    uint64_t cycle,
    std::vector<uint64_t>* completed_instruction_ids) {

    for (std::size_t idx = 0; idx < kCycleInstructionKindCount; ++idx) {
        impl_->components[idx].BeginCycle(cycle, completed_instruction_ids);
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
}

void CycleArch::RecordStall(CycleInstructionKind kind) {
    impl_->ForKind(kind).RecordStall();
}

void CycleArch::EndCycle() {
    for (std::size_t idx = 0; idx < kCycleInstructionKindCount; ++idx) {
        impl_->components[idx].EndCycle();
    }
}

bool CycleArch::HasInflight() const {
    return impl_->HasInflight();
}

std::vector<CycleComponentStats> CycleArch::GetComponentStats() const {
    return impl_->ExportStats();
}
