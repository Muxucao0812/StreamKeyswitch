#pragma once

#include "backend/model/keyswitch_execution_model.h"

#include <cstdint>

enum class HardwareTransferPath : uint8_t {
    HostToHBM,
    HBMToSPM,
    SPMToHBM
};

struct HardwareUnitConfig {
    uint32_t unit_count = 1;
    uint32_t latency_cycles = 1;
    uint32_t pipeline_depth = 1;
    bool full_pipeline = false;
};

struct HardwareConfig {
    double clock_mhz = 300.0;
    uint32_t lanes = 256;
    uint32_t cluster_count = 1;

    double gamma_pattern = 1.0;
    double pcie_write_bytes_per_ns = 12.0;
    double hbm_bytes_per_ns = 32.0;
    Time host_to_hbm_setup_ns = 80;
    Time dma_setup_ns = 60;

    uint32_t mem_fifo_depth = 4;
    uint32_t unit_fifo_depth = 4;
    uint32_t bconv_array_height = 2;
    uint32_t bconv_array_width = 6;

    uint32_t mem_controller_count = 1;
    uint32_t decompose_unit_count = 1;
    uint32_t ntt_unit_count = 1;
    uint32_t ewe_mul_unit_count = 4;
    uint32_t ewe_add_unit_count = 2;
    uint32_t ewe_sub_unit_count = 2;
    uint32_t bconv_unit_count = 12;

    uint32_t mem_pipeline_depth = 4;
    uint32_t decompose_pipeline_depth = 4;
    uint32_t ntt_pipeline_depth = 4;
    uint32_t ewe_pipeline_depth = 4;
    uint32_t bconv_pipeline_depth = 4;

    bool mem_full_pipeline = true;
    bool decompose_full_pipeline = true;
    bool ntt_full_pipeline = true;
    bool ewe_full_pipeline = true;
    bool bconv_full_pipeline = true;

    uint32_t decompose_delay_cycles = 4;
    uint32_t ewe_mul_delay_cycles = 4;
    uint32_t ewe_add_delay_cycles = 2;
    uint32_t ewe_sub_delay_cycles = 2;
    uint32_t butterfly_delay_cycles = 5;
    uint32_t spu_stream_delay_cycles = 2;
    uint32_t intra_transpose_delay_cycles = 16;
    uint32_t inter_transpose_delay_cycles = 256;
    uint32_t bconv_mac_delay_cycles = 20;
    uint32_t bconv_fifo_delay_cycles = 4;

    double energy_hbm_byte_nj = 0.0012;
};

class HardwareModel {
public:
    HardwareModel();
    explicit HardwareModel(const HardwareConfig& config);

    const HardwareConfig& config() const;

    Time CyclesToNs(uint64_t cycles) const;
    uint64_t NsToCycles(Time latency_ns) const;

    uint32_t WavesPerPoly(const KeySwitchProblem& problem) const;
    uint32_t Alpha(const KeySwitchProblem& problem) const;

    uint64_t EstimateTransferCycles(
        HardwareTransferPath path,
        uint64_t bytes) const;

    uint64_t EstimateDecomposeCycles(
        const KeySwitchProblem& problem) const;
    uint64_t EstimateNttCycles(
        const KeySwitchProblem& problem) const;
    uint64_t EstimateInttCycles(
        const KeySwitchProblem& problem) const;
    uint64_t EstimateEweMulCycles(
        const KeySwitchProblem& problem) const;
    uint64_t EstimateEweAddCycles(
        const KeySwitchProblem& problem) const;
    uint64_t EstimateEweSubCycles(
        const KeySwitchProblem& problem) const;
    uint64_t EstimateBconvCycles(
        const KeySwitchProblem& problem) const;

    HardwareUnitConfig MemoryConfig() const;
    HardwareUnitConfig DecomposeConfig() const;
    HardwareUnitConfig NttConfig() const;
    HardwareUnitConfig EweMulConfig() const;
    HardwareUnitConfig EweAddConfig() const;
    HardwareUnitConfig EweSubConfig() const;
    HardwareUnitConfig BconvConfig() const;

    double EstimateTransferEnergyByBytes(uint64_t bytes) const;

private:
    uint64_t TransferSetupCycles(Time setup_ns) const;
    uint64_t EstimateTransferBodyCycles(
        uint64_t bytes,
        double bytes_per_ns) const;
    uint64_t PhaseCycles(
        uint32_t waves_per_poly,
        uint32_t startup_cycles,
        uint32_t per_wave_cycles,
        uint32_t drain_cycles,
        double wave_passes) const;

private:
    HardwareConfig config_;
};
