#pragma once

#include "backend/cycle_sim/instruction.h"
#include "backend/cycle_sim/stats.h"
#include "backend/hw/hardware_model.h"

#include <cstdint>
#include <memory>
#include <vector>

class CycleArch {
public:
    explicit CycleArch(const HardwareModel& hardware);
    ~CycleArch();

    CycleArch(const CycleArch&) = delete;
    CycleArch& operator=(const CycleArch&) = delete;
    CycleArch(CycleArch&&) noexcept;
    CycleArch& operator=(CycleArch&&) noexcept;

    void BeginCycle(
        uint64_t cycle,
        std::vector<uint64_t>* completed_instruction_ids);
    bool CanIssue(
        CycleInstructionKind kind,
        uint64_t cycle) const;
    void Issue(
        const CycleInstruction& instruction,
        uint64_t cycle);
    void RecordStall(CycleInstructionKind kind);
    void EndCycle();

    bool HasInflight() const;
    std::vector<CycleComponentStats> GetComponentStats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
