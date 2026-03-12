#pragma once

#include "backend/execution_backend.h"
#include "backend/profile_table.h"
#include "model/stage.h"

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

struct TableLookupStats {
    uint64_t exact_hits = 0;
    uint64_t interpolated_hits = 0;
    uint64_t nearest_hits = 0;
    uint64_t fallback_hits = 0;

    uint64_t total_queries = 0;

    uint64_t nearest_or_interpolated_hits() const {
        return nearest_hits + interpolated_hits;
    }
};

class TableBackend : public ExecutionBackend {
public:
    TableBackend();
    explicit TableBackend(const std::string& profile_table_csv_path);

    ExecutionResult Estimate(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const override;

    const std::string& ProfileSource() const;

    TableLookupStats GetLookupStats() const;
    void PrintLookupStats(std::ostream& os) const;

private:
    std::vector<Stage> BuildStages(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    bool ResidentKeyHit(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    Time EstimateStageFromTable(
        const Stage& stage,
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        ProfileLookupMode* lookup_mode) const;

private:
    StageProfileTable profile_table_;
    std::string profile_source_;

    mutable TableLookupStats stats_;
};

