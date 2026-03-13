#include "backend/primitive_simulator.h"

#include <algorithm>
#include <limits>

namespace {

PrimitiveType CanonicalPrimitiveType(PrimitiveType type) {
    // Legacy coarse primitive types are canonicalized to fine-grained
    // primitive types so old call sites remain compatible.
    switch (type) {
    case PrimitiveType::KeyLoadHostToHBM:
        return PrimitiveType::KeyLoadHostToHBM;

    case PrimitiveType::KeyLoadHBMToBRAM:
    case PrimitiveType::KeyLoadDMA:
        return PrimitiveType::KeyLoadHBMToBRAM;

    case PrimitiveType::InputHBMToBRAM:
    case PrimitiveType::DispatchDMA:
        return PrimitiveType::InputHBMToBRAM;

    case PrimitiveType::OutputBRAMToHBM:
        return PrimitiveType::OutputBRAMToHBM;

    case PrimitiveType::Decompose:
    case PrimitiveType::DecomposeKernel:
        return PrimitiveType::Decompose;

    case PrimitiveType::NTT:
    case PrimitiveType::NttKernel:
        return PrimitiveType::NTT;

    case PrimitiveType::KSInnerProd:
    case PrimitiveType::KSInnerProdKernel:
    case PrimitiveType::MultiplyKernel:
        return PrimitiveType::KSInnerProd;

    case PrimitiveType::INTT:
    case PrimitiveType::InttKernel:
        return PrimitiveType::INTT;

    case PrimitiveType::Accumulate:
    case PrimitiveType::AccumulateKernel:
        return PrimitiveType::Accumulate;

    case PrimitiveType::Subtract:
        return PrimitiveType::Subtract;

    case PrimitiveType::BConv:
    case PrimitiveType::BasisConvertKernel:
        return PrimitiveType::BConv;

    case PrimitiveType::InterCardSend:
    case PrimitiveType::InterCardComm:
        return PrimitiveType::InterCardSend;

    case PrimitiveType::InterCardRecv:
        return PrimitiveType::InterCardRecv;

    case PrimitiveType::InterCardReduce:
    case PrimitiveType::MergeReduce:
        return PrimitiveType::InterCardReduce;

    case PrimitiveType::Barrier:
    case PrimitiveType::BarrierSync:
        return PrimitiveType::Barrier;
    }

    return PrimitiveType::InputHBMToBRAM;
}

uint32_t CardCount(const PrimitiveOp& op) {
    if (!op.assigned_cards.empty()) {
        return static_cast<uint32_t>(std::max<size_t>(1, op.assigned_cards.size()));
    }

    if (op.fan_in > 1) {
        return op.fan_in;
    }

    const bool has_src = op.src_card >= 0;
    const bool has_dst = op.dst_card >= 0;
    if (has_src && has_dst) {
        return (op.src_card == op.dst_card) ? 1U : 2U;
    }
    if (has_src || has_dst) {
        return 1;
    }
    return 1;
}

uint64_t CeilDivU64(uint64_t a, uint64_t b) {
    return (b == 0) ? 0 : ((a + b - 1) / b);
}

uint64_t EffectiveCommunicationBytes(
    const PrimitiveOp& op,
    uint64_t min_bytes) {

    if (op.bytes > 0) {
        return std::max<uint64_t>(min_bytes, op.bytes);
    }
    if (op.work_units > 0) {
        // Fallback when caller does not provide explicit payload size.
        const uint64_t inferred = CeilDivU64(op.work_units, 8);
        return std::max<uint64_t>(min_bytes, inferred);
    }
    return min_bytes;
}

uint64_t EffectiveFabricBandwidthBytesPerNs(
    uint64_t base_bw_bytes_per_ns,
    uint32_t fan_in,
    uint32_t cards) {

    const uint32_t pressure = std::max<uint32_t>(1, std::max<uint32_t>(fan_in, cards));
    const uint32_t contention_factor =
        1U + ((pressure > 2) ? ((pressure - 2 + 1) / 2) : 0U);
    return std::max<uint64_t>(1, base_bw_bytes_per_ns / contention_factor);
}

Time TransferLatency(
    uint64_t bytes,
    uint64_t bw_bytes_per_ns,
    Time setup_ns,
    uint32_t cards) {

    const uint64_t bw = std::max<uint64_t>(1, bw_bytes_per_ns);
    const Time arbitration_ns = (cards > 1) ? (8 * static_cast<Time>(cards - 1)) : 0;
    return setup_ns + static_cast<Time>(CeilDivU64(bytes, bw)) + arbitration_ns;
}

uint32_t CeilLog2U32(uint32_t value) {
    if (value <= 1) {
        return 0;
    }
    uint32_t levels = 0;
    uint32_t v = value - 1;
    while (v > 0) {
        v >>= 1U;
        ++levels;
    }
    return levels;
}

uint32_t EffectiveFanIn(const PrimitiveOp& op, uint32_t cards) {
    return std::max<uint32_t>(
        std::max<uint32_t>(1, cards),
        std::max<uint32_t>(1, op.fan_in));
}

uint32_t TopologyHopCount(const PrimitiveOp& op, uint32_t fan_in) {
    if (op.src_card >= 0 && op.dst_card >= 0 && op.src_card == op.dst_card) {
        return 0;
    }

    if (fan_in <= 2) {
        return 1;
    }
    if (fan_in <= 4) {
        return 2;
    }
    return 3;
}

Time InterCardSendLatency(const PrimitiveOp& op, uint32_t cards) {
    const uint32_t fan_in = EffectiveFanIn(op, cards);
    const uint32_t hops = TopologyHopCount(op, fan_in);
    const uint64_t bytes = EffectiveCommunicationBytes(op, /*min_bytes=*/64);
    const uint64_t bw_bytes_per_ns = EffectiveFabricBandwidthBytesPerNs(
        /*base_bw_bytes_per_ns=*/26,
        fan_in,
        cards);
    const Time setup_ns = 84 + static_cast<Time>((cards > 1) ? (4 * (cards - 1)) : 0);
    const Time route_ns = 12 + static_cast<Time>(11 * hops);
    const Time serialize_ns = static_cast<Time>(CeilDivU64(bytes, bw_bytes_per_ns));
    const Time contention_ns = (fan_in > 1) ? static_cast<Time>(7 * (fan_in - 1)) : 0;
    return setup_ns + route_ns + serialize_ns + contention_ns;
}

Time InterCardRecvLatency(const PrimitiveOp& op, uint32_t cards) {
    const uint32_t fan_in = EffectiveFanIn(op, cards);
    const uint32_t hops = TopologyHopCount(op, fan_in);
    const uint64_t bytes = EffectiveCommunicationBytes(op, /*min_bytes=*/64);
    const uint64_t bw_bytes_per_ns = EffectiveFabricBandwidthBytesPerNs(
        /*base_bw_bytes_per_ns=*/24,
        fan_in,
        cards);
    const Time setup_ns = 66 + static_cast<Time>((cards > 1) ? (3 * (cards - 1)) : 0);
    const Time route_ns = 10 + static_cast<Time>(10 * hops);
    const Time deserialize_ns = static_cast<Time>(CeilDivU64(bytes, bw_bytes_per_ns));
    const Time queue_ns = (fan_in > 2) ? static_cast<Time>(5 * (fan_in - 2)) : 0;
    return setup_ns + route_ns + deserialize_ns + queue_ns;
}

Time InterCardReduceLatency(const PrimitiveOp& op, uint32_t cards) {
    const uint32_t fan_in = EffectiveFanIn(op, cards);
    if (fan_in <= 1) {
        return 0;
    }

    const uint32_t levels = std::max<uint32_t>(1, CeilLog2U32(fan_in));
    const uint32_t hops = TopologyHopCount(op, fan_in);
    const uint64_t bytes = EffectiveCommunicationBytes(op, /*min_bytes=*/128);
    const uint64_t bw_bytes_per_ns = EffectiveFabricBandwidthBytesPerNs(
        /*base_bw_bytes_per_ns=*/20,
        fan_in,
        cards);
    const uint64_t reduce_work_bytes = std::max<uint64_t>(bytes, CeilDivU64(op.work_units, 4));
    const Time per_level_setup_ns =
        74 + static_cast<Time>(12 * hops) + static_cast<Time>((fan_in > 2) ? (5 * (fan_in - 2)) : 0);
    const Time per_level_transfer_ns = static_cast<Time>(CeilDivU64(bytes, bw_bytes_per_ns));
    const Time per_level_combine_ns = static_cast<Time>(CeilDivU64(
        reduce_work_bytes,
        /*combine_bw_bytes_per_ns=*/18));
    const Time drain_ns = 18 + static_cast<Time>(4 * (levels - 1));
    return static_cast<Time>(levels) * (per_level_setup_ns + per_level_transfer_ns + per_level_combine_ns)
        + drain_ns;
}

Time BarrierLatency(const PrimitiveOp& op, uint32_t cards) {
    const uint32_t fan_in = EffectiveFanIn(op, cards);
    if (fan_in <= 1) {
        return 0;
    }

    const uint32_t levels = std::max<uint32_t>(1, CeilLog2U32(fan_in));
    const uint32_t hops = TopologyHopCount(op, fan_in);
    // Simplified barrier model: tree gather + tree broadcast + skew/jitter.
    const Time gather_ns = static_cast<Time>(levels) * (34 + static_cast<Time>(8 * hops));
    const Time broadcast_ns = static_cast<Time>(levels) * (30 + static_cast<Time>(7 * hops));
    const Time skew_ns = static_cast<Time>(4 * (fan_in - 1));
    const Time jitter_ns = static_cast<Time>((cards > 1) ? (2 * (cards - 1)) : 0);
    return 20 + gather_ns + broadcast_ns + skew_ns + jitter_ns;
}

Time ComputeLatency(
    uint64_t work_units,
    uint64_t throughput_per_ns,
    Time launch_overhead_ns,
    uint32_t cards) {

    const uint64_t denom = std::max<uint64_t>(1, throughput_per_ns);
    const uint32_t workers = std::max<uint32_t>(1, cards);
    const Time scaled_work = static_cast<Time>(CeilDivU64(work_units, workers));
    return launch_overhead_ns + static_cast<Time>(CeilDivU64(scaled_work, denom));
}

PrimitiveStageBreakdown* FindStageBreakdown(
    std::vector<PrimitiveStageBreakdown>* breakdown,
    StageType stage_type) {

    for (auto& entry : *breakdown) {
        if (entry.stage_type == stage_type) {
            return &entry;
        }
    }

    breakdown->push_back(PrimitiveStageBreakdown{stage_type, 0, 0.0});
    return &breakdown->back();
}

PrimitiveTypeBreakdown* FindPrimitiveBreakdown(
    std::vector<PrimitiveTypeBreakdown>* breakdown,
    PrimitiveType primitive_type) {

    for (auto& entry : *breakdown) {
        if (entry.primitive_type == primitive_type) {
            return &entry;
        }
    }

    breakdown->push_back(PrimitiveTypeBreakdown{primitive_type, 0, 0.0, 0, 0});
    return &breakdown->back();
}

} // namespace

const char* ToString(PrimitiveType primitive_type) {
    switch (primitive_type) {
    case PrimitiveType::KeyLoadHostToHBM:
        return "KeyLoadHostToHBM";
    case PrimitiveType::KeyLoadHBMToBRAM:
        return "KeyLoadHBMToBRAM";
    case PrimitiveType::InputHBMToBRAM:
        return "InputHBMToBRAM";
    case PrimitiveType::OutputBRAMToHBM:
        return "OutputBRAMToHBM";
    case PrimitiveType::Decompose:
        return "Decompose";
    case PrimitiveType::NTT:
        return "NTT";
    case PrimitiveType::KSInnerProd:
        return "KSInnerProd";
    case PrimitiveType::Accumulate:
        return "Accumulate";
    case PrimitiveType::Subtract:
        return "Subtract";
    case PrimitiveType::INTT:
        return "INTT";
    case PrimitiveType::BConv:
        return "BConv";
    case PrimitiveType::InterCardSend:
        return "InterCardSend";
    case PrimitiveType::InterCardRecv:
        return "InterCardRecv";
    case PrimitiveType::InterCardReduce:
        return "InterCardReduce";
    case PrimitiveType::Barrier:
        return "Barrier";

    case PrimitiveType::KeyLoadDMA:
        return "KeyLoadDMA";
    case PrimitiveType::DispatchDMA:
        return "DispatchDMA";
    case PrimitiveType::DecomposeKernel:
        return "DecomposeKernel";
    case PrimitiveType::NttKernel:
        return "NttKernel";
    case PrimitiveType::KSInnerProdKernel:
        return "KSInnerProdKernel";
    case PrimitiveType::InttKernel:
        return "InttKernel";
    case PrimitiveType::AccumulateKernel:
        return "AccumulateKernel";
    case PrimitiveType::MultiplyKernel:
        return "MultiplyKernel";
    case PrimitiveType::BasisConvertKernel:
        return "BasisConvertKernel";
    case PrimitiveType::InterCardComm:
        return "InterCardComm";
    case PrimitiveType::BarrierSync:
        return "BarrierSync";
    case PrimitiveType::MergeReduce:
        return "MergeReduce";
    }

    return "DispatchDMA";
}

Time PrimitiveSimulatorStub::EstimatePrimitiveLatency(const PrimitiveOp& op) const {
    const uint32_t cards = CardCount(op);
    const PrimitiveType type = CanonicalPrimitiveType(op.type);

    switch (type) {
    case PrimitiveType::KeyLoadHostToHBM:
        return TransferLatency(
            op.bytes,
            /*bw_bytes_per_ns=*/24,
            /*setup_ns=*/190,
            cards);

    case PrimitiveType::KeyLoadHBMToBRAM:
        return TransferLatency(
            op.bytes,
            /*bw_bytes_per_ns=*/36,
            /*setup_ns=*/72 + (op.key_hit ? 0 : 18),
            cards);

    case PrimitiveType::InputHBMToBRAM:
        return TransferLatency(
            op.bytes,
            /*bw_bytes_per_ns=*/40,
            /*setup_ns=*/65,
            cards);

    case PrimitiveType::OutputBRAMToHBM:
        return TransferLatency(
            op.bytes,
            /*bw_bytes_per_ns=*/34,
            /*setup_ns=*/68,
            cards);

    case PrimitiveType::Decompose:
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/30,
            /*launch_overhead_ns=*/140,
            cards);

    case PrimitiveType::NTT:
        // First-class transform primitive.
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/44,
            /*launch_overhead_ns=*/130,
            cards);

    case PrimitiveType::KSInnerProd:
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/18,
            /*launch_overhead_ns=*/185,
            cards);

    case PrimitiveType::INTT:
        // First-class transform primitive.
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/42,
            /*launch_overhead_ns=*/135,
            cards);

    case PrimitiveType::Accumulate:
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/72,
            /*launch_overhead_ns=*/84,
            cards);

    case PrimitiveType::Subtract:
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/76,
            /*launch_overhead_ns=*/80,
            cards);

    case PrimitiveType::BConv:
        // Basis conversion primitive after transform pair.
        return ComputeLatency(
            op.work_units,
            /*throughput_per_ns=*/28,
            /*launch_overhead_ns=*/155,
            cards);

    case PrimitiveType::InterCardSend:
        return InterCardSendLatency(op, cards);

    case PrimitiveType::InterCardRecv:
        return InterCardRecvLatency(op, cards);

    case PrimitiveType::Barrier:
        return BarrierLatency(op, cards);

    case PrimitiveType::InterCardReduce:
        return InterCardReduceLatency(op, cards);

    default:
        break;
    }

    return 0;
}

double PrimitiveSimulatorStub::EstimatePrimitiveEnergy(
    const PrimitiveOp& op,
    Time latency_ns) const {

    double scale = 1.0;
    const PrimitiveType type = CanonicalPrimitiveType(op.type);
    switch (type) {
    case PrimitiveType::KeyLoadHostToHBM:
        scale = 0.20;
        break;
    case PrimitiveType::KeyLoadHBMToBRAM:
        scale = 0.22;
        break;
    case PrimitiveType::InputHBMToBRAM:
        scale = 0.24;
        break;
    case PrimitiveType::OutputBRAMToHBM:
        scale = 0.25;
        break;
    case PrimitiveType::Decompose:
        scale = 0.48;
        break;
    case PrimitiveType::NTT:
        scale = 0.43;
        break;
    case PrimitiveType::KSInnerProd:
        scale = 0.63;
        break;
    case PrimitiveType::INTT:
        scale = 0.45;
        break;
    case PrimitiveType::Accumulate:
        scale = 0.38;
        break;
    case PrimitiveType::Subtract:
        scale = 0.36;
        break;
    case PrimitiveType::BConv:
        scale = 0.44;
        break;
    case PrimitiveType::InterCardSend:
        scale = 0.30;
        break;
    case PrimitiveType::InterCardRecv:
        scale = 0.27;
        break;
    case PrimitiveType::Barrier:
        scale = 0.15;
        break;
    case PrimitiveType::InterCardReduce:
        scale = 0.34;
        break;
    default:
        break;
    }

    return static_cast<double>(latency_ns) * scale;
}

uint64_t PrimitiveSimulatorStub::EstimatePrimitiveMemory(
    const PrimitiveOp& op,
    const SystemState& state) const {

    uint64_t base = op.bytes;
    if (base == 0) {
        base = static_cast<uint64_t>(op.work_units) * 256ULL;
    }

    const uint32_t cards = CardCount(op);
    if (cards > 1) {
        base = (base / cards) + static_cast<uint64_t>(cards - 1) * 4096ULL;
    }

    uint64_t min_capacity = 0;
    auto update_capacity = [&](CardId card_id) {
        if (card_id >= state.cards.size()) {
            return;
        }
        const auto& card = state.cards[card_id];
        const uint64_t cap =
            (card.bram_capacity_bytes > 0)
            ? card.bram_capacity_bytes
            : card.memory_capacity_bytes;
        if (min_capacity == 0 || cap < min_capacity) {
            min_capacity = cap;
        }
    };

    if (!op.assigned_cards.empty()) {
        for (const CardId card_id : op.assigned_cards) {
            update_capacity(card_id);
        }
    } else {
        if (op.src_card >= 0) {
            update_capacity(static_cast<CardId>(op.src_card));
        }
        if (op.dst_card >= 0 && op.dst_card != op.src_card) {
            update_capacity(static_cast<CardId>(op.dst_card));
        }
    }

    if (min_capacity == 0) {
        return base;
    }

    return std::min<uint64_t>(base, min_capacity);
}

PrimitiveResult PrimitiveSimulatorStub::Simulate(
    const PrimitiveTrace& trace,
    const SystemState& state) const {

    PrimitiveResult result;
    for (const PrimitiveOp& op : trace.ops) {
        const PrimitiveType canonical_type = CanonicalPrimitiveType(op.type);
        const Time latency_ns = EstimatePrimitiveLatency(op);
        const double energy_nj = EstimatePrimitiveEnergy(op, latency_ns);
        const uint64_t memory_bytes = EstimatePrimitiveMemory(op, state);

        result.total_latency_ns += latency_ns;
        result.total_energy_nj += energy_nj;
        result.peak_memory_bytes = std::max(result.peak_memory_bytes, memory_bytes);

        PrimitiveStageBreakdown* stage_entry =
            FindStageBreakdown(&result.stage_breakdown, op.stage_type);
        stage_entry->latency_ns += latency_ns;
        stage_entry->energy_nj += energy_nj;

        PrimitiveTypeBreakdown* primitive_entry =
            FindPrimitiveBreakdown(&result.primitive_breakdown, canonical_type);
        primitive_entry->latency_ns += latency_ns;
        primitive_entry->energy_nj += energy_nj;
        primitive_entry->bytes += op.bytes;
        primitive_entry->work_units += op.work_units;
    }

    return result;
}
