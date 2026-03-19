#include "backend/hw/hardware_model.h"

#include <algorithm>
#include <cmath>
#include <iostream>

namespace {

uint32_t CeilDivU32(uint32_t a, uint32_t b) {
    return (b == 0) ? 0 : ((a + b - 1) / b);
}

uint32_t SafeBconvParallelism(const HardwareConfig& config) {
    // Xiangchen: 目前不考虑bconv单元之间的资源竞争，直接返回总的bconv单元数作为并行度上限。
    return 0;
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
        return TransferSetupCycles(config_.dma_setup_ns) + EstimateTransferBodyCycles(bytes, config_.hbm_bytes_per_ns);
    }

    return 0;
}

uint64_t HardwareModel::EstimateBRAMReadCycles(uint64_t bytes) const {
    if (bytes == 0) {
        return 0;
    }
    return EstimateTransferBodyCycles(
        bytes,
        std::max(1.0, config_.hbm_bytes_per_ns * 4.0));
}

uint64_t HardwareModel::EstimateBRAMWriteCycles(uint64_t bytes) const {
    if (bytes == 0) {
        return 0;
    }
    return EstimateTransferBodyCycles(
        bytes,
        std::max(1.0, config_.hbm_bytes_per_ns * 4.0));
}

uint64_t HardwareModel::EstimateDirectForwardCycles(uint64_t /*bytes*/) const {
    return 0;
}

uint64_t HardwareModel::EstimateDecomposeCycles(
    const KeySwitchProblem& problem, uint32_t num_limbs) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        /*startup_cycles=*/1,
        std::max<uint32_t>(1, config_.spu_stream_delay_cycles),
        /*drain_cycles=*/1,
        /*wave_passes=*/1.0,
        num_limbs);
}

uint64_t HardwareModel::EstimateNttCycles(
    const KeySwitchProblem& problem, uint32_t num_limbs) const {

    const uint32_t waves = WavesPerPoly(problem);
    uint64_t cycles = 0;
    cycles = PhaseCycles(
        /*waves=*/waves,
        /*startup_cycles=*/0,
        /*per_wave_cycles=*/1, //Full Pipeline
        /*drain_cycles=*/config_.butterfly_delay_cycles,
        /*wave_passes=*/std::log2(problem.poly_modulus_degree),
        /*num_limbs=*/num_limbs
    );

  
    return cycles;
}

uint64_t HardwareModel::EstimateInttCycles(
    const KeySwitchProblem& problem, 
    uint32_t num_limbs
) const {

    auto cycles = HardwareModel::EstimateNttCycles(
        problem,
        num_limbs
    );

    return cycles;
}

uint64_t HardwareModel::EstimateEweMulCycles(
    const KeySwitchProblem& problem, uint32_t num_limbs) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        std::max<uint32_t>(1, config_.ewe_mul_delay_cycles),
        1, 0, 1.0, num_limbs);
}

uint64_t HardwareModel::EstimateEweAddCycles(
    const KeySwitchProblem& problem, uint32_t num_limbs) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        std::max<uint32_t>(1, config_.ewe_add_delay_cycles),
        1, 0, 1.0, num_limbs);
}

uint64_t HardwareModel::EstimateEweSubCycles(
    const KeySwitchProblem& problem, uint32_t num_limbs) const {

    const uint32_t waves = WavesPerPoly(problem);
    return PhaseCycles(
        waves,
        std::max<uint32_t>(1, config_.ewe_sub_delay_cycles),
        1, 0, 1.0, num_limbs);
}

uint64_t HardwareModel::EstimateBconvCycles(
    const KeySwitchProblem& problem, 
    uint32_t num_input_limbs,
    uint32_t num_output_limbs
) const {

    // BConv = 矩阵乘法：l 个输入 limbs → k 个输出 limbs。
    // 硬件：256 lanes，每 lane 1 个模乘 + 1 个模加。
    // 每个 clock 处理 1 个输入 limb 的 1 个 wave（256 个系数）。
    // 产出 1 个输出 limb = l × waves 个 clock。
    // 产出 k 个输出 limb = k × l × waves 个 clock。

    const uint32_t waves = WavesPerPoly(problem);
    auto cycles = PhaseCycles(
        /*waves=*/waves*num_output_limbs*num_input_limbs,
        /*startup_cycles=*/std::max<uint32_t>(1, config_.bconv_mac_delay_cycles),
        /*per_wave_cycles=*/1,
        /*drain_cycles=*/0,
        /*wave_passes=*/1.0,
        /*num_limbs=*/SafeBconvParallelism(config_)
    );
    return cycles;
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

HardwareUnitConfig HardwareModel::ComputeArrayConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.compute_array_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.compute_array_latency_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.compute_array_pipeline_depth);
    unit.full_pipeline = config_.compute_array_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::SpuConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.spu_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.spu_latency_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.spu_pipeline_depth);
    unit.full_pipeline = config_.spu_full_pipeline;
    return unit;
}

HardwareUnitConfig HardwareModel::InterconnectConfig() const {
    HardwareUnitConfig unit;
    unit.unit_count = std::max<uint32_t>(1, config_.interconnect_link_count);
    unit.latency_cycles = std::max<uint32_t>(1, config_.interconnect_latency_cycles);
    unit.pipeline_depth = std::max<uint32_t>(1, config_.interconnect_pipeline_depth);
    unit.full_pipeline = config_.interconnect_full_pipeline;
    return unit;
}

uint64_t HardwareModel::EstimateInterconnectTransferCycles(uint64_t bytes) const {
    if (bytes == 0) {
        return 0;
    }
    return TransferSetupCycles(config_.interconnect_setup_ns)
        + EstimateTransferBodyCycles(bytes, config_.interconnect_bytes_per_ns);
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
    double wave_passes,
    uint32_t num_limbs) const {

    const uint32_t limbs = std::max<uint32_t>(1, num_limbs);
    const uint64_t phase_waves = static_cast<uint64_t>(
        std::ceil(static_cast<double>(std::max<uint32_t>(1, waves_per_poly)) * wave_passes - 1e-12));
    const uint64_t body = phase_waves * static_cast<uint64_t>(std::max<uint32_t>(1, per_wave_cycles));
    return static_cast<uint64_t>(startup_cycles)
        + body * static_cast<uint64_t>(limbs)
        + static_cast<uint64_t>(drain_cycles);
}
