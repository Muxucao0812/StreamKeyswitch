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
    uint32_t num_k = ((num_rns_limbs + 1) / num_digits) + 1;
    uint32_t key_component_count = 2;
    uint32_t bytes_per_coeff = 8;

    // Xiangchen: This bandwidth should be pcie bandwidth
    Time key_load_base_time = 144;
    uint32_t key_load_bandwidth_bytes_per_ns = 32;

    uint64_t ComputeKeyBytes() const {
        const uint64_t raw_bytes =
            static_cast<uint64_t>(poly_modulus_degree)
            * static_cast<uint64_t>(num_rns_limbs + num_k)
            * static_cast<uint64_t>(num_digits)
            * static_cast<uint64_t>(key_component_count)
            * static_cast<uint64_t>(bytes_per_coeff);
     
        return raw_bytes;
    }

    Time ComputeKeyLoadTime() const {
        const uint64_t bandwidth =
            std::max<uint64_t>(1, key_load_bandwidth_bytes_per_ns);
        return key_load_base_time
            + static_cast<Time>((ComputeKeyBytes() + bandwidth - 1) / bandwidth);
    }

    static HEParams BuiltInDefault() {
        return HEParams{};
    }
};
