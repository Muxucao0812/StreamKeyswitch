#include "backend/profile_table.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

std::string Trim(const std::string& text) {
    size_t begin = 0;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin]))) {
        ++begin;
    }

    if (begin == text.size()) {
        return "";
    }

    size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        --end;
    }
    return text.substr(begin, end - begin);
}

std::string ToLower(std::string text) {
    for (char& ch : text) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return text;
}

bool ParseUint64(const std::string& text, uint64_t* out) {
    if (text.empty()) {
        return false;
    }

    uint64_t value = 0;
    for (const char ch : text) {
        if (!std::isdigit(static_cast<unsigned char>(ch))) {
            return false;
        }

        const uint64_t digit = static_cast<uint64_t>(ch - '0');
        if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10ULL) {
            return false;
        }
        value = value * 10ULL + digit;
    }

    *out = value;
    return true;
}

bool ParseUint32(const std::string& text, uint32_t* out) {
    uint64_t wide = 0;
    if (!ParseUint64(text, &wide)) {
        return false;
    }
    if (wide > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
        return false;
    }
    *out = static_cast<uint32_t>(wide);
    return true;
}

bool ParseBool(const std::string& text, bool* out) {
    const std::string lower = ToLower(Trim(text));
    if (lower == "1" || lower == "true" || lower == "hit") {
        *out = true;
        return true;
    }
    if (lower == "0" || lower == "false" || lower == "miss") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseDouble(const std::string& text, double* out) {
    if (text.empty()) {
        return false;
    }

    char* end = nullptr;
    const double value = std::strtod(text.c_str(), &end);
    if (end == text.c_str() || *end != '\0') {
        return false;
    }
    *out = value;
    return true;
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::string current;

    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (ch == '"') {
            in_quotes = !in_quotes;
            continue;
        }

        if (ch == ',' && !in_quotes) {
            fields.push_back(Trim(current));
            current.clear();
            continue;
        }

        current.push_back(ch);
    }

    fields.push_back(Trim(current));
    return fields;
}

bool BucketCompatible(
    const StageLatencyProfileEntry& entry,
    const StageLatencyLookupQuery& query) {

    if (entry.input_bytes_bucket != 0
        && query.input_bytes_bucket != 0
        && entry.input_bytes_bucket != query.input_bytes_bucket) {
        return false;
    }

    if (entry.output_bytes_bucket != 0
        && query.output_bytes_bucket != 0
        && entry.output_bytes_bucket != query.output_bytes_bucket) {
        return false;
    }

    return true;
}

uint64_t Distance(
    const StageLatencyProfileEntry& entry,
    const StageLatencyLookupQuery& query) {

    const uint64_t d_digits = static_cast<uint64_t>(
        std::abs(static_cast<int>(entry.num_digits) - static_cast<int>(query.num_digits)));
    const uint64_t d_rns = static_cast<uint64_t>(
        std::abs(static_cast<int>(entry.num_rns_limbs) - static_cast<int>(query.num_rns_limbs)));
    const uint64_t d_cards = static_cast<uint64_t>(
        std::abs(static_cast<int>(entry.card_count) - static_cast<int>(query.card_count)));

    uint64_t d_in_bucket = 0;
    if (entry.input_bytes_bucket != 0 && query.input_bytes_bucket != 0) {
        d_in_bucket = static_cast<uint64_t>(
            std::abs(static_cast<int>(entry.input_bytes_bucket)
            - static_cast<int>(query.input_bytes_bucket)));
    }

    uint64_t d_out_bucket = 0;
    if (entry.output_bytes_bucket != 0 && query.output_bytes_bucket != 0) {
        d_out_bucket = static_cast<uint64_t>(
            std::abs(static_cast<int>(entry.output_bytes_bucket)
            - static_cast<int>(query.output_bytes_bucket)));
    }

    return 100ULL * d_digits
        + 10ULL * d_rns
        + 2ULL * d_cards
        + 1ULL * d_in_bucket
        + 1ULL * d_out_bucket;
}

std::vector<StageLatencyProfileEntry> BuildDefaultProfiles() {
    std::vector<StageLatencyProfileEntry> profiles;
    profiles.reserve(256);

    const std::vector<uint32_t> digits_values = {2, 3, 4};
    const std::vector<uint32_t> rns_values = {4, 6};
    const std::vector<uint32_t> cards_values = {1, 2, 4};

    for (const uint32_t digits : digits_values) {
        for (const uint32_t rns : rns_values) {
            for (const uint32_t cards : cards_values) {
                for (const bool hit : {false, true}) {
                    const Time miss_penalty = hit ? 0 : (80 + 20 * cards);

                    Time key_load = hit
                        ? (20 + 8 * cards)
                        : (320 + 85 * digits + 22 * rns + 65 * cards);
                    profiles.push_back({
                        StageType::KeyLoad,
                        digits,
                        rns,
                        cards,
                        hit,
                        key_load});

                    Time dispatch = 55 + 14 * digits + 4 * rns
                        + (cards > 1 ? (12 * (cards - 1)) : 0)
                        + miss_penalty / 3;
                    profiles.push_back({
                        StageType::Dispatch,
                        digits,
                        rns,
                        cards,
                        hit,
                        dispatch});

                    Time decompose = (260 + 95 * digits + 12 * rns + miss_penalty) / cards
                        + 90
                        + (cards > 1 ? (12 * (cards - 1)) : 0);
                    profiles.push_back({
                        StageType::Decompose,
                        digits,
                        rns,
                        cards,
                        hit,
                        decompose});

                    Time multiply = (460 + 145 * digits + 16 * rns + miss_penalty) / cards
                        + 170
                        + (cards > 1 ? (18 * (cards - 1)) : 0);
                    profiles.push_back({
                        StageType::Multiply,
                        digits,
                        rns,
                        cards,
                        hit,
                        multiply});

                    Time basis_convert = (180 + 64 * digits + 32 * rns + miss_penalty / 2) / cards
                        + 70
                        + (cards > 1 ? (9 * (cards - 1)) : 0);
                    profiles.push_back({
                        StageType::BasisConvert,
                        digits,
                        rns,
                        cards,
                        hit,
                        basis_convert});

                    Time merge = (cards == 1)
                        ? 0
                        : (140 + 58 * cards + 9 * digits + 4 * rns + miss_penalty / 2);
                    profiles.push_back({
                        StageType::Merge,
                        digits,
                        rns,
                        cards,
                        hit,
                        merge});
                }
            }
        }
    }

    return profiles;
}

} // namespace

const char* ToString(StageType stage_type) {
    switch (stage_type) {
    case StageType::KeyLoad:
        return "KeyLoad";
    case StageType::Dispatch:
        return "Dispatch";
    case StageType::Decompose:
        return "Decompose";
    case StageType::Multiply:
        return "Multiply";
    case StageType::BasisConvert:
        return "BasisConvert";
    case StageType::Merge:
        return "Merge";
    }
    return "Dispatch";
}

bool ParseStageType(const std::string& text, StageType* stage_type) {
    const std::string lower = ToLower(Trim(text));
    if (lower == "keyload" || lower == "key_load") {
        *stage_type = StageType::KeyLoad;
        return true;
    }
    if (lower == "dispatch") {
        *stage_type = StageType::Dispatch;
        return true;
    }
    if (lower == "decompose") {
        *stage_type = StageType::Decompose;
        return true;
    }
    if (lower == "multiply") {
        *stage_type = StageType::Multiply;
        return true;
    }
    if (lower == "basisconvert" || lower == "basis_convert") {
        *stage_type = StageType::BasisConvert;
        return true;
    }
    if (lower == "merge") {
        *stage_type = StageType::Merge;
        return true;
    }
    return false;
}

StageProfileTable StageProfileTable::BuildBuiltInDefault() {
    StageProfileTable table;
    table.SetEntries(BuildDefaultProfiles());
    return table;
}

void StageProfileTable::SetEntries(std::vector<StageLatencyProfileEntry> entries) {
    entries_ = std::move(entries);
}

const std::vector<StageLatencyProfileEntry>& StageProfileTable::Entries() const {
    return entries_;
}

StageLatencyLookupResult StageProfileTable::Lookup(
    const StageLatencyLookupQuery& query) const {

    StageLatencyLookupResult out;

    if (entries_.empty()) {
        out.mode = ProfileLookupMode::NotFound;
        return out;
    }

    std::vector<const StageLatencyProfileEntry*> stage_and_hit_candidates;
    stage_and_hit_candidates.reserve(entries_.size());

    std::vector<const StageLatencyProfileEntry*> stage_candidates;
    stage_candidates.reserve(entries_.size());

    for (const auto& entry : entries_) {
        if (entry.stage_type != query.stage_type) {
            continue;
        }
        stage_candidates.push_back(&entry);
        if (entry.key_hit == query.key_hit) {
            stage_and_hit_candidates.push_back(&entry);
        }
    }

    if (stage_candidates.empty()) {
        out.mode = ProfileLookupMode::NotFound;
        return out;
    }

    for (const auto* entry : stage_and_hit_candidates) {
        if (entry->num_digits == query.num_digits
            && entry->num_rns_limbs == query.num_rns_limbs
            && entry->card_count == query.card_count
            && BucketCompatible(*entry, query)) {
            out.found = true;
            out.mode = ProfileLookupMode::Exact;
            out.latency_ns = entry->latency_ns;
            return out;
        }
    }

    std::vector<const StageLatencyProfileEntry*> exact_dim;
    exact_dim.reserve(stage_and_hit_candidates.size());
    for (const auto* entry : stage_and_hit_candidates) {
        if (entry->num_digits == query.num_digits
            && entry->num_rns_limbs == query.num_rns_limbs
            && BucketCompatible(*entry, query)) {
            exact_dim.push_back(entry);
        }
    }

    if (!exact_dim.empty()) {
        std::sort(
            exact_dim.begin(),
            exact_dim.end(),
            [](const StageLatencyProfileEntry* a, const StageLatencyProfileEntry* b) {
                return a->card_count < b->card_count;
            });

        const StageLatencyProfileEntry* lower = nullptr;
        const StageLatencyProfileEntry* upper = nullptr;
        for (const auto* entry : exact_dim) {
            if (entry->card_count <= query.card_count) {
                lower = entry;
            }
            if (entry->card_count >= query.card_count) {
                upper = entry;
                break;
            }
        }

        if (lower != nullptr && upper != nullptr) {
            if (lower->card_count == upper->card_count) {
                out.found = true;
                out.mode = ProfileLookupMode::Exact;
                out.latency_ns = lower->latency_ns;
                return out;
            }

            const double x0 = static_cast<double>(lower->card_count);
            const double x1 = static_cast<double>(upper->card_count);
            const double y0 = static_cast<double>(lower->latency_ns);
            const double y1 = static_cast<double>(upper->latency_ns);
            const double x = static_cast<double>(query.card_count);
            const double y = y0 + (x - x0) * (y1 - y0) / (x1 - x0);

            out.found = true;
            out.mode = ProfileLookupMode::Interpolated;
            out.latency_ns = static_cast<Time>(std::llround(std::max(y, 0.0)));
            return out;
        }

        if (lower != nullptr) {
            out.found = true;
            out.mode = ProfileLookupMode::Nearest;
            out.latency_ns = lower->latency_ns;
            return out;
        }

        if (upper != nullptr) {
            out.found = true;
            out.mode = ProfileLookupMode::Nearest;
            out.latency_ns = upper->latency_ns;
            return out;
        }
    }

    uint64_t best_distance = std::numeric_limits<uint64_t>::max();
    const StageLatencyProfileEntry* best = nullptr;

    for (const auto* entry : stage_candidates) {
        const uint64_t key_hit_penalty = (entry->key_hit == query.key_hit) ? 0ULL : 1000ULL;
        const uint64_t distance = Distance(*entry, query) + key_hit_penalty;
        if (distance < best_distance) {
            best_distance = distance;
            best = entry;
        }
    }

    if (best != nullptr) {
        out.found = true;
        out.mode = ProfileLookupMode::Nearest;
        out.latency_ns = best->latency_ns;
        return out;
    }

    out.mode = ProfileLookupMode::NotFound;
    return out;
}

ProfileTableLoadResult LoadStageProfileTableFromCsv(const std::string& path) {
    ProfileTableLoadResult result;
    result.source = path;

    std::ifstream in(path);
    if (!in.is_open()) {
        result.error_message = "Failed to open profile table CSV: " + path;
        return result;
    }

    std::string header_line;
    size_t header_line_no = 0;
    while (std::getline(in, header_line)) {
        ++header_line_no;
        const std::string trimmed = Trim(header_line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        header_line = trimmed;
        break;
    }

    if (header_line.empty()) {
        result.error_message = "Profile table CSV is empty: " + path;
        return result;
    }

    const std::vector<std::string> headers_raw = SplitCsvLine(header_line);
    std::vector<std::string> headers;
    headers.reserve(headers_raw.size());
    for (const auto& h : headers_raw) {
        headers.push_back(ToLower(Trim(h)));
    }

    std::unordered_map<std::string, size_t> col;
    for (size_t i = 0; i < headers.size(); ++i) {
        col[headers[i]] = i;
    }

    const std::vector<std::string> required = {
        "stage_type",
        "num_digits",
        "num_rns_limbs",
        "card_count",
        "key_hit",
        "latency_ns"};

    for (const auto& field : required) {
        if (col.find(field) == col.end()) {
            result.error_message = "Profile table CSV missing required column: " + field;
            return result;
        }
    }

    std::vector<StageLatencyProfileEntry> entries;

    std::string line;
    size_t line_no = header_line_no;
    while (std::getline(in, line)) {
        ++line_no;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }

        const std::vector<std::string> fields = SplitCsvLine(trimmed);

        auto get = [&](const std::string& name) -> std::string {
            const auto it = col.find(name);
            if (it == col.end()) {
                return "";
            }
            if (it->second >= fields.size()) {
                return "";
            }
            return Trim(fields[it->second]);
        };

        StageLatencyProfileEntry entry;

        const std::string stage_text = get("stage_type");
        if (!ParseStageType(stage_text, &entry.stage_type)) {
            result.error_message =
                "Profile table parse error at line " + std::to_string(line_no)
                + ": invalid stage_type '" + stage_text + "'";
            return result;
        }

        if (!ParseUint32(get("num_digits"), &entry.num_digits)) {
            result.error_message =
                "Profile table parse error at line " + std::to_string(line_no)
                + ": invalid num_digits";
            return result;
        }

        if (!ParseUint32(get("num_rns_limbs"), &entry.num_rns_limbs)) {
            result.error_message =
                "Profile table parse error at line " + std::to_string(line_no)
                + ": invalid num_rns_limbs";
            return result;
        }

        if (!ParseUint32(get("card_count"), &entry.card_count) || entry.card_count == 0) {
            result.error_message =
                "Profile table parse error at line " + std::to_string(line_no)
                + ": invalid card_count";
            return result;
        }

        if (!ParseBool(get("key_hit"), &entry.key_hit)) {
            result.error_message =
                "Profile table parse error at line " + std::to_string(line_no)
                + ": invalid key_hit";
            return result;
        }

        if (!ParseUint64(get("latency_ns"), &entry.latency_ns)) {
            result.error_message =
                "Profile table parse error at line " + std::to_string(line_no)
                + ": invalid latency_ns";
            return result;
        }

        if (col.find("input_bytes_bucket") != col.end()) {
            const std::string text = get("input_bytes_bucket");
            if (!text.empty()) {
                if (!ParseUint32(text, &entry.input_bytes_bucket)) {
                    result.error_message =
                        "Profile table parse error at line " + std::to_string(line_no)
                        + ": invalid input_bytes_bucket";
                    return result;
                }
            }
        }

        if (col.find("output_bytes_bucket") != col.end()) {
            const std::string text = get("output_bytes_bucket");
            if (!text.empty()) {
                if (!ParseUint32(text, &entry.output_bytes_bucket)) {
                    result.error_message =
                        "Profile table parse error at line " + std::to_string(line_no)
                        + ": invalid output_bytes_bucket";
                    return result;
                }
            }
        }

        if (col.find("energy_nj") != col.end()) {
            const std::string text = get("energy_nj");
            if (!text.empty()) {
                if (!ParseDouble(text, &entry.energy_nj)) {
                    result.error_message =
                        "Profile table parse error at line " + std::to_string(line_no)
                        + ": invalid energy_nj";
                    return result;
                }
                entry.has_energy_nj = true;
            }
        }

        if (col.find("memory_bytes") != col.end()) {
            const std::string text = get("memory_bytes");
            if (!text.empty()) {
                if (!ParseUint64(text, &entry.memory_bytes)) {
                    result.error_message =
                        "Profile table parse error at line " + std::to_string(line_no)
                        + ": invalid memory_bytes";
                    return result;
                }
                entry.has_memory_bytes = true;
            }
        }

        entries.push_back(entry);
    }

    if (entries.empty()) {
        result.error_message = "Profile table CSV has no data rows: " + path;
        return result;
    }

    result.ok = true;
    result.loaded_rows = entries.size();
    result.entries = std::move(entries);
    return result;
}

