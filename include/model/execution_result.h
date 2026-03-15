#pragma once

#include "common/types.h"
#include "model/keyswitch_reason.h"
#include "model/request.h"

#include <vector>
#include <string>

struct ExecutionPlan {
    RequestId request_id = 0;
    std::vector<CardId> assigned_cards;
};

struct ExecutionBreakdown {
    Time queue_time = 0;
    Time key_load_time = 0;
    Time dispatch_time = 0;
    Time decompose_time = 0;
    Time multiply_time = 0;
    Time basis_convert_time = 0;
    Time merge_time = 0;
};

struct TransferBreakdown {
    Time key_host_to_hbm_time = 0;
    Time key_hbm_to_bram_time = 0;
    Time input_hbm_to_bram_time = 0;
    Time output_bram_to_hbm_time = 0;
};

struct PrimitiveComputeBreakdown {
    Time transform_time = 0;
    Time ntt_time = 0;
    Time intt_time = 0;
    Time bconv_time = 0;
    Time inner_product_time = 0;
    Time accumulate_time = 0;
    Time subtract_time = 0;
};

struct CommunicationBreakdown {
    Time inter_card_send_time = 0;
    Time inter_card_recv_time = 0;
    Time inter_card_reduce_time = 0;
    Time inter_card_barrier_time = 0;

    uint64_t inter_card_send_bytes = 0;
    uint64_t inter_card_recv_bytes = 0;
    uint64_t inter_card_reduce_bytes = 0;
};

struct PeakBufferBreakdown {
    uint64_t persistent_peak_bytes = 0;
    uint64_t static_peak_bytes = 0;
    uint64_t dynamic_peak_bytes = 0;
    uint64_t key_peak_bytes = 0;
    uint64_t ct_peak_bytes = 0;
    uint64_t out_peak_bytes = 0;
    uint64_t temp_peak_bytes = 0;
};

struct ExecutionResult {
    // Primary latency for primitive-cycle path.
    // CycleBackend sets this from PrimitiveResult::total_latency_ns directly.
    Time total_latency = 0;
    // Legacy compatibility alias. For primitive-cycle path this mirrors peak_total_bytes.
    Time peak_memory_bytes = 0;
    double energy_nj = 0.0;

    // Peak memory metrics for primitive-cycle semantics.
    // - peak_bram_bytes: on-chip BRAM occupancy peak from execution/tile model.
    // - peak_hbm_bytes: coarse non-BRAM peak from primitive simulator memory model.
    // - peak_total_bytes: max(peak_bram_bytes, peak_hbm_bytes), the primary peak metric.
    uint64_t peak_bram_bytes = 0;
    uint64_t peak_hbm_bytes = 0;
    uint64_t peak_total_bytes = 0;
    // Raw primitive simulator peak (kept to expose simulator-internal estimate).
    uint64_t primitive_peak_memory_bytes = 0;

    uint64_t hbm_read_bytes = 0;
    uint64_t hbm_write_bytes = 0;
    uint64_t bram_read_bytes = 0;
    uint64_t bram_write_bytes = 0;
    uint64_t key_host_to_hbm_bytes = 0;
    uint64_t key_hbm_to_bram_bytes = 0;
    uint64_t ct_hbm_to_bram_bytes = 0;
    uint64_t out_bram_to_hbm_bytes = 0;
    uint64_t hbm_round_trips = 0;
    uint64_t compute_cycles = 0;
    uint64_t transfer_cycles = 0;
    uint64_t direct_forward_count = 0;
    uint64_t direct_forward_bytes = 0;
    uint64_t spill_count = 0;
    uint64_t reload_count = 0;
    uint64_t spill_bytes = 0;
    uint64_t reload_bytes = 0;
    uint64_t working_set_bytes = 0;
    uint32_t tile_count = 0;
    uint64_t dependency_stall_cycles = 0;
    uint64_t resource_stall_cycles = 0;
    std::vector<Time> fine_step_cycles;
    bool key_resident_reuse = false;
    bool key_resident_hit = false;
    bool key_persistent_bram = false;
    bool fallback_used = false;
    // Machine-readable fallback reason. Valid when fallback_used=true.
    KeySwitchFallbackReason fallback_reason = KeySwitchFallbackReason::None;
    // Human-readable alias of fallback_reason for CLI/log/reporting.
    std::string fallback_reason_message;

    // Method-level execution intent/effectiveness:
    // - requested_method: method requested by input profile.
    // - effective_method: method actually used by execution model.
    // - method_degraded=true indicates successful execution with changed method.
    //   Example: requested Cinnamon but effective Poseidon.
    // - degraded_reason explains why method_degraded happened.
    KeySwitchMethod requested_method = KeySwitchMethod::Auto;
    KeySwitchMethod effective_method = KeySwitchMethod::Auto;
    bool method_degraded = false;
    KeySwitchFallbackReason degraded_reason = KeySwitchFallbackReason::None;
    std::string degraded_reason_message;
    // Derived execution status flags for debugging/assertion convenience.
    // They are normalized by backend result filling logic.
    bool unsupported_method = false;
    bool unsupported_config = false;
    bool degraded_to_single_board = false;
    bool compatibility_fallback = false;
    bool normal_execution = false;
    bool tiled_execution = false;

    // Semantic markers:
    // - primitive_breakdown_primary=true: transfer/compute/communication breakdowns
    //   are the main result and are derived from primitive simulation.
    // - stage_breakdown_compat_only=true: StageType breakdown is compatibility view only
    //   (derived from primitive simulation, never re-aggregated into total_latency).
    bool primitive_breakdown_primary = false;
    bool stage_breakdown_compat_only = false;

    TransferBreakdown transfer_breakdown;
    PrimitiveComputeBreakdown compute_breakdown;
    CommunicationBreakdown communication_breakdown;
    PeakBufferBreakdown peak_buffers;
    // Compatibility stage-level view (coarse, aggregated).
    ExecutionBreakdown breakdown;
};
