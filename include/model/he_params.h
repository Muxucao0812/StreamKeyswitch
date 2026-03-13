#pragma once

#include "common/types.h"

#include <algorithm>
#include <cstdint>
#include <string>

struct HEParams {
    std::string name = "built-in default";

    uint32_t poly_modulus_degree = 65536;
    uint32_t num_polys = 2;
    uint32_t num_rns_limbs = 23;
    uint32_t num_digits = 3;
    uint32_t key_component_count = 2;
    uint32_t bytes_per_coeff = 8;
    uint32_t key_storage_divisor = 1;

    // Xiangchen: This bandwidth should be pcie bandwidth
    Time key_load_base_time = 144;
    uint32_t key_load_bandwidth_bytes_per_ns = 32;

    uint32_t ComputeNumK(
        uint32_t rns_limb_count,
        uint32_t digit_width_limbs) const {
        const uint32_t safe_digits = std::max<uint32_t>(1, digit_width_limbs);
        return ((rns_limb_count + 1) / safe_digits) + 1;
    }

    uint64_t ComputeCiphertextBytes(
        uint32_t ciphertext_count,
        uint32_t poly_count,
        uint32_t rns_limb_count) const {
        return static_cast<uint64_t>(std::max<uint32_t>(1, ciphertext_count))
            * static_cast<uint64_t>(std::max<uint32_t>(1, poly_count))
            * static_cast<uint64_t>(poly_modulus_degree)
            * static_cast<uint64_t>(std::max<uint32_t>(1, rns_limb_count))
            * static_cast<uint64_t>(bytes_per_coeff);
    }

    uint64_t ComputeKeyBytes(
        uint32_t digit_width_limbs,
        uint32_t rns_limb_count) const {
        const uint32_t safe_digits = std::max<uint32_t>(1, digit_width_limbs);
        const uint32_t safe_rns_limbs = std::max<uint32_t>(1, rns_limb_count);
        const uint32_t num_k = ComputeNumK(safe_rns_limbs, safe_digits);
        const uint64_t raw_bytes =
            static_cast<uint64_t>(poly_modulus_degree)
            * static_cast<uint64_t>(safe_rns_limbs + num_k)
            * static_cast<uint64_t>(safe_digits)
            * static_cast<uint64_t>(key_component_count)
            * static_cast<uint64_t>(bytes_per_coeff);

        const uint64_t divisor = std::max<uint64_t>(1, key_storage_divisor);
        return std::max<uint64_t>(1, (raw_bytes + divisor - 1) / divisor);
    }

    uint64_t ComputeKeyBytes() const {
        return ComputeKeyBytes(num_digits, num_rns_limbs);
    }

    Time ComputeKeyLoadTime() const {
        const uint64_t bandwidth = std::max<uint64_t>(1, key_load_bandwidth_bytes_per_ns);
        return key_load_base_time + static_cast<Time>((ComputeKeyBytes() + bandwidth - 1) / bandwidth);
    }

    static HEParams BuiltInDefault() {
        return HEParams{};
    }
};
