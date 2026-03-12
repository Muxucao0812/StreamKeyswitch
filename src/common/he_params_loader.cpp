#include "common/he_params_loader.h"

#include <cctype>
#include <cstdint>
#include <fstream>
#include <limits>
#include <string>

namespace {

std::string Trim(const std::string& text) {
    size_t begin = 0;
    while (begin < text.size()
        && std::isspace(static_cast<unsigned char>(text[begin]))) {
        ++begin;
    }

    size_t end = text.size();
    while (end > begin
        && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
        --end;
    }

    return text.substr(begin, end - begin);
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

} // namespace

HEParamsLoadResult LoadHEParamsFromFile(const std::string& path) {
    HEParamsLoadResult result;
    result.source = path;

    std::ifstream in(path);
    if (!in.is_open()) {
        result.error_message = "Failed to open HE params file: " + path;
        return result;
    }

    HEParams params = HEParams::BuiltInDefault();
    params.name = path;

    std::string line;
    size_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;

        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }

        const size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            result.error_message =
                path + ":" + std::to_string(line_no) + ": expected key=value";
            return result;
        }

        const std::string key = Trim(trimmed.substr(0, eq));
        const std::string value = Trim(trimmed.substr(eq + 1));

        if (key == "name") {
            params.name = value;
            continue;
        }

        uint32_t u32 = 0;
        uint64_t u64 = 0;

        if (key == "poly_modulus_degree") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid poly_modulus_degree";
                return result;
            }
            params.poly_modulus_degree = u32;
            continue;
        }

        if (key == "num_digits") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid num_digits";
                return result;
            }
            params.num_digits = u32;
            continue;
        }

        if (key == "num_rns_limbs") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid num_rns_limbs";
                return result;
            }
            params.num_rns_limbs = u32;
            continue;
        }

        if (key == "num_polys") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid num_polys";
                return result;
            }
            params.num_polys = u32;
            continue;
        }

        if (key == "key_component_count") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid key_component_count";
                return result;
            }
            params.key_component_count = u32;
            continue;
        }

        if (key == "bytes_per_coeff") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid bytes_per_coeff";
                return result;
            }
            params.bytes_per_coeff = u32;
            continue;
        }

        if (key == "key_load_base_time") {
            if (!ParseUint64(value, &u64)) {
                result.error_message =
                    path + ":" + std::to_string(line_no) + ": invalid key_load_base_time";
                return result;
            }
            params.key_load_base_time = u64;
            continue;
        }

        if (key == "key_load_bandwidth_bytes_per_ns") {
            if (!ParseUint32(value, &u32) || u32 == 0) {
                result.error_message =
                    path + ":" + std::to_string(line_no)
                    + ": invalid key_load_bandwidth_bytes_per_ns";
                return result;
            }
            params.key_load_bandwidth_bytes_per_ns = u32;
            continue;
        }

        result.error_message =
            path + ":" + std::to_string(line_no) + ": unknown key " + key;
        return result;
    }

    result.ok = true;
    result.params = params;
    return result;
}
