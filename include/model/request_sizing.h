#pragma once

#include "model/request.h"

#include <algorithm>
#include <cstdint>
#include <limits>

inline constexpr uint64_t kAlveoU280HbmBytes = 8ULL * 1024ULL * 1024ULL * 1024ULL;
inline constexpr uint64_t kAlveoU280BramBytes = 32ULL * 1024ULL * 1024ULL;

inline uint64_t EstimateRequestWorkingSetBytes(const Request& req) {
    return req.ks_profile.input_bytes + req.ks_profile.key_bytes;
}

inline uint32_t RecommendCardCountForWorkingSet(
    uint64_t working_set_bytes,
    uint64_t per_card_memory_bytes = kAlveoU280HbmBytes) {
    const uint64_t usable_per_card = std::max<uint64_t>(1, per_card_memory_bytes);
    const uint64_t required_cards =
        std::max<uint64_t>(1, (working_set_bytes + usable_per_card - 1) / usable_per_card);
    return static_cast<uint32_t>(std::min<uint64_t>(required_cards, std::numeric_limits<uint32_t>::max()));
}

inline uint32_t RecommendCardCountForRequest(
    const Request& req,
    uint64_t per_card_memory_bytes = kAlveoU280HbmBytes) {
    return RecommendCardCountForWorkingSet(EstimateRequestWorkingSetBytes(req), per_card_memory_bytes);
}

inline uint32_t DecideCardCountForRequest(
    const Request& req,
    uint64_t per_card_memory_bytes = kAlveoU280HbmBytes
) {
    if (req.ks_profile.method == KeySwitchMethod::SingleBoardClassic) {
        return 1;
    }

    const uint32_t recommended_cards = RecommendCardCountForRequest(req, per_card_memory_bytes);
    const uint32_t preferred_cards = std::max<uint32_t>(1, req.ks_profile.preferred_cards);
    uint32_t desired_cards = std::max<uint32_t>(recommended_cards, preferred_cards);
    if (IsCinnamonMethod(req.ks_profile.method)) {
        desired_cards = std::max<uint32_t>(
            desired_cards,
            std::max<uint32_t>(1, req.ks_profile.num_digits));
    }

    const uint32_t max_cards = (req.ks_profile.max_cards == 0)
        ? desired_cards
        : req.ks_profile.max_cards;
    return std::max<uint32_t>(1, std::min(desired_cards, max_cards));
}
