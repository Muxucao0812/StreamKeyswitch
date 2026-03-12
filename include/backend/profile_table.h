#pragma once

#include "model/stage.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

enum class ProfileLookupMode {
    Exact,
    Interpolated,
    Nearest,
    NotFound
};

struct StageLatencyProfileEntry {
    StageType stage_type = StageType::Dispatch;
    uint32_t num_digits = 1;
    uint32_t num_rns_limbs = 1;
    uint32_t card_count = 1;
    bool key_hit = false;
    Time latency_ns = 0;

    uint32_t input_bytes_bucket = 0;
    uint32_t output_bytes_bucket = 0;

    double energy_nj = 0.0;
    bool has_energy_nj = false;

    uint64_t memory_bytes = 0;
    bool has_memory_bytes = false;
};

struct StageLatencyLookupQuery {
    StageType stage_type = StageType::Dispatch;
    uint32_t num_digits = 1;
    uint32_t num_rns_limbs = 1;
    uint32_t card_count = 1;
    bool key_hit = false;

    uint32_t input_bytes_bucket = 0;
    uint32_t output_bytes_bucket = 0;
};

struct StageLatencyLookupResult {
    bool found = false;
    ProfileLookupMode mode = ProfileLookupMode::NotFound;
    Time latency_ns = 0;
};

struct ProfileTableLoadResult {
    bool ok = false;
    std::string source;
    size_t loaded_rows = 0;
    std::vector<StageLatencyProfileEntry> entries;
    std::string error_message;
};

class StageProfileTable {
public:
    StageProfileTable() = default;

    static StageProfileTable BuildBuiltInDefault();

    void SetEntries(std::vector<StageLatencyProfileEntry> entries);

    const std::vector<StageLatencyProfileEntry>& Entries() const;

    StageLatencyLookupResult Lookup(const StageLatencyLookupQuery& query) const;

private:
    std::vector<StageLatencyProfileEntry> entries_;
};

ProfileTableLoadResult LoadStageProfileTableFromCsv(const std::string& path);

bool ParseStageType(const std::string& text, StageType* stage_type);
const char* ToString(StageType stage_type);

