#include "backend/cycle_backend/cycle_backend_cinnamon_ib.h"

#include "backend/cycle_backend/cycle_backend_primitives.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace {

struct DigitShard {
    uint32_t begin = 0;
    uint32_t count = 0;
};

std::vector<DigitShard> BuildDigitShards(
    uint32_t total_digits,
    uint32_t cards) {

    const uint32_t safe_digits = std::max<uint32_t>(1, total_digits);
    const uint32_t safe_cards = std::max<uint32_t>(1, std::min<uint32_t>(cards, safe_digits));
    std::vector<DigitShard> shards;
    shards.reserve(safe_cards);
    for (uint32_t idx = 0; idx < safe_cards; ++idx) {
        const uint32_t begin = static_cast<uint32_t>(
            (static_cast<uint64_t>(idx) * safe_digits) / safe_cards);
        const uint32_t end = static_cast<uint32_t>(
            (static_cast<uint64_t>(idx + 1) * safe_digits) / safe_cards);
        shards.push_back(DigitShard{begin, end - begin});
    }
    return shards;
}

uint64_t ScaleBytesByRatio(
    uint64_t total_bytes,
    uint32_t part,
    uint32_t whole) {

    if (total_bytes == 0) {
        return 0;
    }
    const uint32_t safe_whole = std::max<uint32_t>(1, whole);
    const __uint128_t numer =
        static_cast<__uint128_t>(total_bytes) * static_cast<__uint128_t>(part)
        + static_cast<__uint128_t>(safe_whole - 1);
    const __uint128_t scaled_128 = numer / static_cast<__uint128_t>(safe_whole);
    if (scaled_128 > static_cast<__uint128_t>(std::numeric_limits<uint64_t>::max())) {
        return std::numeric_limits<uint64_t>::max();
    }
    return std::max<uint64_t>(1, static_cast<uint64_t>(scaled_128));
}

uint32_t Emit(
    CycleProgramBuilder* builder,
    const std::string& name,
    CycleInstructionKind kind,
    CycleTransferPath transfer_path,
    CycleOpType type,
    uint64_t bytes,
    uint64_t work_items,
    const std::vector<uint32_t>& deps) {

    CyclePrimitiveDesc desc;
    desc.name = name;
    desc.transfer_path = transfer_path;
    desc.type = type;
    desc.bytes = bytes;
    desc.input_limbs = 1;
    desc.output_limbs = 1;
    desc.work_items = work_items;
    desc.deps = deps;
    return builder->EmitPrimitive(kind, desc);
}

} // namespace

CycleProgram BuildCinnamonInputBroadcastProgram(
    const KeySwitchProblem& problem,
    const HardwareModel& hardware) {

    if (!problem.valid
        || !IsCinnamonMethod(problem.method)
        || problem.multi_board_mode != MultiBoardMode::InputBroadcast
        || problem.active_cards <= 1
        || problem.partition_strategy != PartitionStrategy::ByDigit
        || problem.key_placement != KeyPlacement::ShardedByPartition
        || problem.collective_strategy != CollectiveStrategy::GatherToRoot) {
        return CycleProgram{};
    }

    CycleProgramBuilder builder(
        problem,
        hardware,
        problem.method,
        "cinnamon_input_broadcast_keyswitch");

    const uint32_t active_cards = std::max<uint32_t>(1, problem.active_cards);
    const std::vector<DigitShard> shards = BuildDigitShards(problem.digits, active_cards);
    if (shards.size() != active_cards) {
        return CycleProgram{};
    }

    const uint32_t invalid_group = std::numeric_limits<uint32_t>::max();
    std::vector<uint32_t> input_ready(active_cards, invalid_group);
    std::vector<uint32_t> card_terminal(active_cards, invalid_group);

    input_ready[0] = Emit(
        &builder,
        "root_load_input",
        CycleInstructionKind::LoadHBM,
        CycleTransferPath::HBMToSPM,
        CycleOpType::DataLoad,
        problem.input_bytes,
        problem.ciphertexts,
        {});

    for (uint32_t card_idx = 1; card_idx < active_cards; ++card_idx) {
        const uint32_t send_group = Emit(
            &builder,
            "broadcast_send_card_" + std::to_string(card_idx),
            CycleInstructionKind::InterCardSend,
            CycleTransferPath::None,
            CycleOpType::InterCardComm,
            problem.input_bytes,
            1,
            {input_ready[0]});
        input_ready[card_idx] = Emit(
            &builder,
            "broadcast_recv_card_" + std::to_string(card_idx),
            CycleInstructionKind::InterCardRecv,
            CycleTransferPath::None,
            CycleOpType::InterCardComm,
            problem.input_bytes,
            1,
            {send_group});
    }

    for (uint32_t card_idx = 0; card_idx < active_cards; ++card_idx) {
        const DigitShard& shard = shards[card_idx];
        const uint64_t shard_key_bytes = ScaleBytesByRatio(problem.key_bytes, shard.count, problem.digits);
        const uint64_t shard_work_items =
            std::max<uint64_t>(1, static_cast<uint64_t>(problem.ciphertexts) * shard.count);
        const uint64_t transform_bytes =
            std::max<uint64_t>(problem.ct_limb_bytes, static_cast<uint64_t>(shard.count) * problem.ct_limb_bytes);

        const uint32_t key_load = Emit(
            &builder,
            "load_key_card_" + std::to_string(card_idx),
            CycleInstructionKind::LoadHBM,
            CycleTransferPath::HBMToSPM,
            CycleOpType::KeyLoad,
            shard_key_bytes,
            shard_work_items,
            {input_ready[card_idx]});

        const uint32_t intt = Emit(
            &builder,
            "intt_card_" + std::to_string(card_idx),
            CycleInstructionKind::INTT,
            CycleTransferPath::None,
            CycleOpType::INTT,
            transform_bytes,
            shard_work_items,
            {input_ready[card_idx], key_load});

        const uint32_t bconv = Emit(
            &builder,
            "bconv_card_" + std::to_string(card_idx),
            CycleInstructionKind::BConv,
            CycleTransferPath::None,
            CycleOpType::BConv,
            transform_bytes,
            shard_work_items,
            {intt});

        const uint32_t ntt = Emit(
            &builder,
            "ntt_card_" + std::to_string(card_idx),
            CycleInstructionKind::NTT,
            CycleTransferPath::None,
            CycleOpType::NTT,
            transform_bytes,
            shard_work_items,
            {bconv});

        uint32_t terminal = Emit(
            &builder,
            "mul_card_" + std::to_string(card_idx),
            CycleInstructionKind::EweMul,
            CycleTransferPath::None,
            CycleOpType::Multiply,
            std::max(problem.output_bytes, transform_bytes),
            std::max<uint64_t>(1, static_cast<uint64_t>(problem.polys) * shard_work_items),
            {ntt, key_load});

        if (shard.count > 1) {
            terminal = Emit(
                &builder,
                "local_reduce_card_" + std::to_string(card_idx),
                CycleInstructionKind::EweAdd,
                CycleTransferPath::None,
                CycleOpType::Add,
                problem.output_bytes,
                std::max<uint64_t>(1, static_cast<uint64_t>(shard.count - 1) * problem.polys),
                {terminal});
        }

        card_terminal[card_idx] = terminal;
    }

    std::vector<uint32_t> reduce_deps;
    reduce_deps.reserve(active_cards);
    reduce_deps.push_back(card_terminal[0]);
    for (uint32_t card_idx = 1; card_idx < active_cards; ++card_idx) {
        const uint32_t send_group = Emit(
            &builder,
            "partial_send_card_" + std::to_string(card_idx),
            CycleInstructionKind::InterCardSend,
            CycleTransferPath::None,
            CycleOpType::InterCardComm,
            problem.output_bytes,
            1,
            {card_terminal[card_idx]});
        const uint32_t recv_group = Emit(
            &builder,
            "partial_recv_card_" + std::to_string(card_idx),
            CycleInstructionKind::InterCardRecv,
            CycleTransferPath::None,
            CycleOpType::InterCardComm,
            problem.output_bytes,
            1,
            {send_group});
        reduce_deps.push_back(recv_group);
    }

    const uint32_t reduce_group = Emit(
        &builder,
        "root_reduce",
        CycleInstructionKind::InterCardReduce,
        CycleTransferPath::None,
        CycleOpType::InterCardComm,
        static_cast<uint64_t>(active_cards) * problem.output_bytes,
        active_cards,
        reduce_deps);

    const uint32_t barrier_group = Emit(
        &builder,
        "root_barrier",
        CycleInstructionKind::InterCardSend,
        CycleTransferPath::None,
        CycleOpType::InterCardComm,
        0,
        0,
        {reduce_group});

    Emit(
        &builder,
        "store_output",
        CycleInstructionKind::StoreHBM,
        CycleTransferPath::SPMToHBM,
        CycleOpType::Spill,
        problem.output_bytes,
        problem.ciphertexts,
        {barrier_group});

    return builder.Ok() ? builder.program : CycleProgram{};
}
