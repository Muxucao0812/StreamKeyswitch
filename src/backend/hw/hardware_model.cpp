#include "backend/hw/hardware_model.h"

#include <algorithm>
#include <cmath>

namespace {

uint32_t CeilDivU32(uint32_t a, uint32_t b) {
    return (b == 0) ? 0 : ((a + b - 1) / b);
}

uint32_t SafeBconvParallelism(const HardwareConfig& config) {
    const uint32_t array_parallelism = std::max<uint32_t>(
        1,
        config.bconv_array_height * config.bconv_array_width);
    return std::max<uint32_t>(config.bconv_unit_count, array_parallelism);
}

} // namespace

HardwareModel::HardwareModel()
    : config_(HardwareConfig{}) {}

HardwareModel::HardwareModel(const HardwareConfig& config)
    : config_(config) {}

const HardwareConfig& HardwareModel::config() const {
    return config_;
}

Time HardwareModel::CyclesToNs(uint64_t cycles) const {
    if (cycles == 0) {
        return 0;
    }

    const double safe_clock = std::max(1.0, config_.clock_mhz);
    return static_cast<Time>(
        std::ceil(static_cast<double>(cycles) * 1000.0 / safe_clock));
}

uint64_t HardwareModel::NsToCycles(Time latency_ns) const {
    if (latency_ns == 0) {
        return 0;
    }

    const double safe_clock = std::max(1.0, config_.clock_mhz);
    return static_cast<uint64_t>(
        std::ceil(static_cast<double>(latency_ns) * safe_clock / 1000.0));
}

uint32_t HardwareModel::WavesPerPoly(const KeySwitchProblem& problem) const {
    return std::max<uint32_t>(
        1,
        CeilDivU32(
            std::max<uint32_t>(1, problem.poly_modulus_degree),
            std::max<uint32_t>(1, config_.lanes)));
}

uint32_t HardwareModel::Alpha(const KeySwitchProblem& problem) const {
    return std::max<uint32_t>(
        1,
        CeilDivU32(
            std::max<uint32_t>(1, problem.limbs + 1),
            std::max<uint32_t>(1, problem.digits)));
}

uint64_t HardwareModel::EstimateTransferCycles(
    HardwareTransferPath path,
    uint64_t bytes) const {

    if (bytes == 0) {
        return 0;
    }

    switch (path) {
    case HardwareTransferPath::HostToHBM:
        return TransferSetupCycles(config_.host_to_hbm_setup_ns)
            + EstimateTransferBodyCycles(bytes, config_.pcie_write_bytes_per_ns);
    case HardwareTransferPath::HBMToSPM:
    case HardwareTransferPath::SPMToHBM:
        return TransferSetupCycles(config_.dma_setup_ns)
            + EstimateTransferBodyCycles(bytes, config_.hbm_bytes_per_ns);
    }

    return 0;
}

uint64_t HardwareModel::EstimateDecomposeCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        /*startup_cycles=*/1,
        std::max<uint32_t>(1, config_.spu_stream_delay_cycles),
        /*drain_cycles=*/1,
        /*wave_passes=*/1.0);
}

uint64_t HardwareModel::EstimateNttCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    uint64_t cycles = 0;
    cycles += PhaseCycles(waves, 0, 1, 0, 1.0);
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/1,
        std::max<uint32_t>(1, config_.spu_stream_delay_cycles),
        /*drain_cycles=*/1,
        /*wave_passes=*/1.0);
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/0,
        std::max<uint32_t>(1, config_.butterfly_delay_cycles),
        /*drain_cycles=*/0,
        /*wave_passes=*/8.0);
    cycles += config_.intra_transpose_delay_cycles;
    cycles += PhaseCycles(waves, 0, 1, 0, 1.0);
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/1,
        std::max<uint32_t>(1, config_.spu_stream_delay_cycles),
        /*drain_cycles=*/1,
        /*wave_passes=*/1.0);
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/0,
        std::max<uint32_t>(1, config_.butterfly_delay_cycles),
        /*drain_cycles=*/0,
        /*wave_passes=*/8.0);
    cycles += config_.inter_transpose_delay_cycles;
    return cycles;
}

uint64_t HardwareModel::EstimateInttCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    uint64_t cycles = 0;
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/1,
        std::max<uint32_t>(1, config_.spu_stream_delay_cycles),
        /*drain_cycles=*/1,
        /*wave_passes=*/1.0);
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/0,
        std::max<uint32_t>(1, config_.butterfly_delay_cycles),
        /*drain_cycles=*/0,
        /*wave_passes=*/8.0);
    cycles += PhaseCycles(waves, 0, 1, 0, 1.0);
    cycles += config_.intra_transpose_delay_cycles;
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/1,
        std::max<uint32_t>(1, config_.spu_stream_delay_cycles),
        /*drain_cycles=*/1,
        /*wave_passes=*/1.0);
    cycles += PhaseCycles(
        waves,
        /*startup_cycles=*/0,
        std::max<uint32_t>(1, config_.butterfly_delay_cycles),
        /*drain_cycles=*/0,
        /*wave_passes=*/8.0);
    cycles += PhaseCycles(waves, 0, 1, 0, 1.0);
    cycles += config_.inter_transpose_delay_cycles;
    return cycles;
}

uint64_t HardwareModel::EstimateEweMulCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        std::max<uint32_t>(1, config_.ewe_mul_delay_cycles),
        /*per_wave_cycles=*/1,
        /*drain_cycles=*/0,
        /*wave_passes=*/1.0);
}

uint64_t HardwareModel::EstimateEweAddCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        std::max<uint32_t>(1, config_.ewe_add_delay_cycles),
        /*per_wave_cycles=*/1,
        /*drain_cycles=*/0,
        /*wave_passes=*/1.0);
}

uint64_t HardwareModel::EstimateEweSubCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        std::max<uint32_t>(1, config_.ewe_sub_delay_cycles),
        /*per_wave_cycles=*/1,
        /*drain_cycles=*/0,
        /*wave_passes=*/1.0);
}

uint64_t HardwareModel::EstimateBconvCycles(
    const KeySwitchProblem& problem) const {

    const uint32_t waves = WavesPerPoly(problem);
    const uint32_t alpha = Alpha(problem);
    const uint32_t passes = CeilDivU32(
        alpha,
        std::max<uint32_t>(1, config_.bconv_array_height));
    const uint32_t per_wave_cycles = std::max<uint32_t>(
        1,
        config_.bconv_array_width / 2);
    return PhaseCycles(
               waves,
               std::max<uint32_t>(1, config_.bconv_fifo_delay_cycles),
               per_wave_cycles,
               /*drain_cycles=*/2,
               static_cast<double>(passes))
        + std::max<uint32_t>(1, config_.bconv_mac_delay_cycles);
}

HardwareUnitConfig HardwareModel::MemoryConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.mem_controller_count);
    unit.latency_cycles = 1;
    unit.pipeline_depth = std::max<uint32_t>(1, config_.mem_pipeline_depth);
    unit.full_pipeline = config_.mem_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::DecomposeConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.decompose_unit_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.decompose_delay_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.decompose_pipeline_depth);
    unit.full_pipeline = config_.decompose_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::NttConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.ntt_unit_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.butterfly_delay_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.ntt_pipeline_depth);
    unit.full_pipeline = config_.ntt_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::EweMulConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.ewe_mul_unit_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.ewe_mul_delay_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.ewe_pipeline_depth);
    unit.full_pipeline = config_.ewe_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::EweAddConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.ewe_add_unit_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.ewe_add_delay_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.ewe_pipeline_depth);
    unit.full_pipeline = config_.ewe_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::EweSubConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.ewe_sub_unit_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.ewe_sub_delay_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.ewe_pipeline_depth);
    unit.full_pipeline = config_.ewe_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::BconvConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = SafeBconvParallelism(config_);
    unit.latency_cycles = std::max<uint32_t>(1, config_.bconv_mac_delay_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.bconv_pipeline_depth);
    unit.full_pipeline = config_.bconv_full_pipeline;
    return unit;
}

double HardwareModel::EstimateTransferEnergyByBytes(uint64_t bytes) const {
    return static_cast<double>(bytes) * config_.energy_hbm_byte_nj;
}

uint64_t HardwareModel::TransferSetupCycles(Time setup_ns) const {
    return NsToCycles(setup_ns);
}

uint64_t HardwareModel::EstimateTransferBodyCycles(
    uint64_t bytes,
    double bytes_per_ns) const {

    if (bytes == 0) {
        return 0;
    }

    const double safe_bw = std::max(1e-9, bytes_per_ns);
    const double safe_clock = std::max(1.0, config_.clock_mhz);
    const double penalty = std::max(1.0, config_.gamma_pattern);
    return static_cast<uint64_t>(
        std::ceil(static_cast<double>(bytes) * penalty * safe_clock / (safe_bw * 1000.0)));
}

uint64_t HardwareModel::PhaseCycles(
    uint32_t waves_per_poly,
    uint32_t startup_cycles,
    uint32_t per_wave_cycles,
    uint32_t drain_cycles,
    double wave_passes) const {

    const uint64_t phase_waves = static_cast<uint64_t>(
        std::ceil(static_cast<double>(std::max<uint32_t>(1, waves_per_poly)) * wave_passes - 1e-12));
    return static_cast<uint64_t>(startup_cycles)
        + phase_waves * static_cast<uint64_t>(std::max<uint32_t>(1, per_wave_cycles))
        + static_cast<uint64_t>(drain_cycles);
}
