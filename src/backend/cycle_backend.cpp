#include "backend/cycle_backend.h"

#include <ostream>
#include <string>

namespace {

const char* KeySwitchMethodName(KeySwitchMethod method) {
    switch (method) {
    case KeySwitchMethod::Auto:
        return "Auto";
    case KeySwitchMethod::SingleBoardClassic:
        return "SingleBoardClassic";
    case KeySwitchMethod::SingleBoardFused:
        return "SingleBoardFused";
    case KeySwitchMethod::ScaleOutLimb:
        return "ScaleOutLimb";
    case KeySwitchMethod::ScaleOutDigit:
        return "ScaleOutDigit";
    case KeySwitchMethod::ScaleOutCiphertext:
        return "ScaleOutCiphertext";
    case KeySwitchMethod::Poseidon:
        return "Poseidon";
    case KeySwitchMethod::OLA:
        return "OLA";
    case KeySwitchMethod::FAB:
        return "FAB";
    case KeySwitchMethod::FAST:
        return "FAST";
    case KeySwitchMethod::HERA:
        return "HERA";
    case KeySwitchMethod::Cinnamon:
        return "Cinnamon";
    }

    return "Unknown";
}

void NormalizeStatusFlags(ExecutionResult* result) {
    if (result->fallback_reason != KeySwitchFallbackReason::None
        && result->fallback_reason_message.empty()) {
        result->fallback_reason_message = ToString(result->fallback_reason);
    }
    if (result->degraded_reason != KeySwitchFallbackReason::None
        && result->degraded_reason_message.empty()) {
        result->degraded_reason_message = ToString(result->degraded_reason);
    }

    result->unsupported_method =
        result->fallback_used
        && result->fallback_reason == KeySwitchFallbackReason::UnsupportedMethod;
    result->unsupported_config =
        result->fallback_used
        && result->fallback_reason == KeySwitchFallbackReason::UnsupportedConfig;
    result->compatibility_fallback =
        result->fallback_used
        && result->fallback_reason == KeySwitchFallbackReason::LegacyStageFallback;
    result->degraded_to_single_board =
        result->method_degraded
        && result->degraded_reason == KeySwitchFallbackReason::DegradedToSingleBoard;
    result->normal_execution = !result->fallback_used && !result->method_degraded;
}

ExecutionResult MakeFallbackResult(
    const Request& req,
    KeySwitchMethod effective_method,
    KeySwitchFallbackReason reason) {

    ExecutionResult result{};
    result.requested_method = req.ks_profile.method;
    result.effective_method = effective_method;

    result.fallback_used = (reason != KeySwitchFallbackReason::None);
    result.fallback_reason = reason;

    result.method_degraded = false;
    result.degraded_reason = KeySwitchFallbackReason::None;
    result.tiled_execution = false;

    // Keep primary-path semantics explicit even for stubs/fallbacks.
    result.primitive_breakdown_primary = true;
    result.stage_breakdown_compat_only = true;

    // Minimal compatibility fill for downstream metric/report consumers.
    result.tile_count = 1;
    result.key_host_to_hbm_bytes = req.ks_profile.key_bytes;
    result.key_hbm_to_bram_bytes = 0;
    result.ct_hbm_to_bram_bytes = req.ks_profile.input_bytes;
    result.out_bram_to_hbm_bytes = req.ks_profile.output_bytes;
    result.hbm_read_bytes =
        result.key_host_to_hbm_bytes
        + result.key_hbm_to_bram_bytes
        + result.ct_hbm_to_bram_bytes;
    result.hbm_write_bytes = result.out_bram_to_hbm_bytes;
    result.working_set_bytes = req.ks_profile.input_bytes + req.ks_profile.key_bytes;

    NormalizeStatusFlags(&result);
    return result;
}

ExecutionResult MakeMethodStubNotImplemented(
    const Request& req,
    const ExecutionPlan& plan,
    KeySwitchMethod method) {

    if (plan.assigned_cards.empty()) {
        return MakeFallbackResult(req, method, KeySwitchFallbackReason::NoAssignedCard);
    }

    ExecutionResult result = MakeFallbackResult(
        req,
        method,
        KeySwitchFallbackReason::UnsupportedConfig);
    result.fallback_reason_message =
        std::string(ToString(result.fallback_reason))
        + ": " + KeySwitchMethodName(method)
        + "_stub_not_implemented";
    NormalizeStatusFlags(&result);
    return result;
}

} // namespace

KeySwitchMethod CycleBackend::ResolveKeySwitchMethod(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& /*state*/) const {

    const KeySwitchMethod requested = req.ks_profile.method;
    if (requested != KeySwitchMethod::Auto) {
        return requested;
    }

    // Skeleton default policy for Auto:
    // - multi-card assigned -> Cinnamon
    // - single/no card assigned -> Poseidon
    return (plan.assigned_cards.size() > 1)
        ? KeySwitchMethod::Cinnamon
        : KeySwitchMethod::Poseidon;
}

ExecutionResult CycleBackend::Estimate(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    ++stats_.estimate_calls;

    const KeySwitchMethod method = ResolveKeySwitchMethod(req, plan, state);

    ExecutionResult result{};
    switch (method) {
    case KeySwitchMethod::Poseidon:
        result = EstimatePoseidon(req, plan, state);
        break;

    case KeySwitchMethod::FAB:
        result = EstimateFAB(req, plan, state);
        break;

    case KeySwitchMethod::FAST:
        result = EstimateFAST(req, plan, state);
        break;

    case KeySwitchMethod::OLA:
        result = EstimateOLA(req, plan, state);
        break;

    case KeySwitchMethod::HERA:
        result = EstimateHERA(req, plan, state);
        break;

    case KeySwitchMethod::Cinnamon:
        result = EstimateCinnamon(req, plan, state);
        break;

    default:
        result = MakeFallbackResult(req, method, KeySwitchFallbackReason::UnsupportedMethod);
        break;
    }

    NormalizeStatusFlags(&result);
    if (result.fallback_used) {
        ++stats_.fallback_count;
    }
    return result;
}

ExecutionResult CycleBackend::EstimatePoseidon(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    (void)state;
    return MakeMethodStubNotImplemented(req, plan, KeySwitchMethod::Poseidon);
}

ExecutionResult CycleBackend::EstimateFAB(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    (void)state;
    return MakeMethodStubNotImplemented(req, plan, KeySwitchMethod::FAB);
}

ExecutionResult CycleBackend::EstimateFAST(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    (void)state;
    return MakeMethodStubNotImplemented(req, plan, KeySwitchMethod::FAST);
}

ExecutionResult CycleBackend::EstimateOLA(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    (void)state;
    return MakeMethodStubNotImplemented(req, plan, KeySwitchMethod::OLA);
}

ExecutionResult CycleBackend::EstimateHERA(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    (void)state;
    return MakeMethodStubNotImplemented(req, plan, KeySwitchMethod::HERA);
}

ExecutionResult CycleBackend::EstimateCinnamon(
    const Request& req,
    const ExecutionPlan& plan,
    const SystemState& state) const {

    (void)state;
    return MakeMethodStubNotImplemented(req, plan, KeySwitchMethod::Cinnamon);
}

CycleBackendStats CycleBackend::GetStats() const {
    return stats_;
}

void CycleBackend::PrintStats(std::ostream& os) const {
    const CycleBackendStats s = GetStats();

    os << "=== CycleBackend Method Dispatch Stats ===\n";
    os << "EstimateCalls=" << s.estimate_calls << "\n";
    os << "CycleFallbackCount=" << s.fallback_count << "\n";
}
