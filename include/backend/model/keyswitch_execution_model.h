#pragma once

#include "backend/model/keyswitch_method_policy.h"
#include "model/execution_result.h"
#include "model/keyswitch_reason.h"
#include "model/request.h"
#include "model/stage.h"
#include "model/system_state.h"

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

// Cinnamon 类多卡执行的 digit 分片描述。
struct DigitShard {
    uint32_t begin = 0;
    uint32_t count = 0;
};

// 将 total_digits 尽量均衡地分到 cards 张卡上，允许不整除。
std::vector<DigitShard> BuildDigitShards(
    uint32_t total_digits,
    uint32_t cards);

// KeySwitch 问题描述：由请求 + 执行计划 + 系统状态归一化后的规模信息。
// 该结构是后续 tile 规划与执行图构建的基础输入。
struct KeySwitchProblem {
    // 问题是否有效（例如是否分配到卡、参数是否合法）。
    bool valid = false;
    // 所需密钥是否命中设备驻留缓存。
    bool key_resident_hit = false;
    // 当前问题采用的 keyswitch 方法。
    KeySwitchMethod method = KeySwitchMethod::Poseidon;
    // Multi-board metadata used by direct program builders.
    MultiBoardMode multi_board_mode = MultiBoardMode::Sequential;
    PartitionStrategy partition_strategy = PartitionStrategy::None;
    KeyPlacement key_placement = KeyPlacement::StreamFromHBM;
    CollectiveStrategy collective_strategy = CollectiveStrategy::None;
    uint32_t active_cards = 1;
    std::vector<DigitShard> digit_shards;

    // 并行资源与数据维度。
    uint32_t cards = 1;
    uint32_t ciphertexts = 1;
    uint32_t digits = 1;
    uint32_t limbs = 1;
    // 扩展模数维度：k = ceil((l + 1) / a), key_limbs = l + k。
    uint32_t num_k = 1;
    uint32_t digit_limbs = 1;
    uint32_t key_limbs = 2;
    uint32_t polys = 1;
    uint32_t poly_modulus_degree = 1;

    // 全量输入/输出/密钥字节规模。
    uint64_t input_bytes = 0;
    uint64_t output_bytes = 0;
    uint64_t key_bytes = 0;

    // 归一化后的最小粒度字节规模（按 limb 或 digit*limb）。
    uint64_t ct_limb_bytes = 1;
    uint64_t out_limb_bytes = 1;
    uint64_t key_digit_limb_bytes = 1;

    // BRAM 容量与预算。
    uint64_t min_card_bram_capacity_bytes = 0;
    uint64_t bram_budget_bytes = 0;
    uint64_t bram_guard_bytes = 0;
    // 临时缓冲系数，用于估算 temp 工作空间。
    double temp_buffer_ratio = 0.5;

    // 本次 keyswitch 的工作集大小估计。
    uint64_t working_set_bytes = 0;
};

// 缓冲组成分解：按数据语义拆分占用。
struct BufferBreakdown {
    uint64_t key_buffer_bytes = 0;            // 密钥相关缓存
    uint64_t ciphertext_buffer_bytes = 0;     // 密文相关缓存
    uint64_t temp_working_buffer_bytes = 0;   // 临时工作缓存
    uint64_t accumulation_buffer_bytes = 0;   // 累加/归并缓存
};

// 某时刻或某步骤的缓冲使用快照。
struct BufferUsage {
    // 持久/静态/动态三类占用分解。
    BufferBreakdown persistent;
    BufferBreakdown tile_static;
    BufferBreakdown dynamic;

    // 三类占用总字节。
    uint64_t persistent_bytes = 0;
    uint64_t static_bytes = 0;
    uint64_t dynamic_working_bytes = 0;

    // 按数据类别汇总后的字节。
    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    uint64_t out_bytes = 0;
    uint64_t temp_bytes = 0;
    // 当前 live 总占用（主要用于步骤前后快照）。
    uint64_t total_live_bytes = 0;
};

// 峰值缓冲统计：记录整个执行中观测到的最大值。
struct PeakBufferUsage {
    // 各组件峰值（分语义）。
    BufferBreakdown component_peak;

    // 按持久/静态/动态维度统计峰值。
    uint64_t persistent_peak_bytes = 0;
    uint64_t static_peak_bytes = 0;
    uint64_t dynamic_peak_bytes = 0;

    // 按 key/ct/out/temp 维度统计峰值。
    uint64_t key_peak_bytes = 0;
    uint64_t ct_peak_bytes = 0;
    uint64_t out_peak_bytes = 0;
    uint64_t temp_peak_bytes = 0;
    // 总峰值。
    uint64_t total_peak_bytes = 0;
};

// Tile 候选方案（用于 planner 比较与筛选）。
struct TileCandidate {
    bool valid = false;                       // 候选是否有效
    bool key_persistent = false;              // key 是否常驻 BRAM
    uint32_t ct_tile = 1;                     // ct 维 tile 大小
    uint32_t limb_tile = 1;                   // limb 维 tile 大小
    uint32_t digit_tile = 1;                  // digit 维 tile 大小
    uint64_t score = 0;                       // 候选评分（偏好更大覆盖）
    uint64_t estimated_peak_bram_bytes = 0;   // 预计 BRAM 峰值
    double estimated_total_cost = 0.0;        // 预计总成本
};

// 单个 ct_tile 的缓冲占用统计。
struct TileBufferUsage {
    uint32_t ct_tile_index = 0;               // 第几个 ct tile
    uint32_t ct_count = 0;                    // 该 tile 覆盖的 ct 数量

    // 分层缓冲分解。
    BufferBreakdown persistent_buffers;
    BufferBreakdown static_buffers;
    BufferBreakdown dynamic_peak_buffers;
    uint64_t persistent_bytes = 0;
    uint64_t static_bytes = 0;
    uint64_t dynamic_working_bytes = 0;

    // 按数据类别汇总。
    uint64_t key_bytes = 0;
    uint64_t ct_bytes = 0;
    uint64_t out_bytes = 0;
    uint64_t temp_bytes = 0;
    uint64_t peak_live_bytes = 0;
};

// 代价分解：用于解释 planner 最终选择依据。
struct TileCostBreakdown {
    double tile_overhead_cost = 0.0;      // 每 tile 固定开销
    double key_transfer_cost = 0.0;       // key 搬运开销
    double ct_transfer_cost = 0.0;        // 输入 ct 搬运开销
    double output_store_cost = 0.0;       // 输出回写开销
    double decompose_compute_cost = 0.0;  // decompose 计算开销
    double multiply_compute_cost = 0.0;   // 乘法/内积/累加计算开销
    double basis_convert_cost = 0.0;      // 基变换计算开销
    double total_cost = 0.0;              // 总成本（加权后）
};

// Planner 输出的最终 tile 方案。
struct TilePlan {
    bool valid = false;                   // 方案是否有效
    bool key_persistent = false;          // key 是否常驻 BRAM
    uint32_t ct_tile = 1;                 // ct 维 tile 大小
    uint32_t limb_tile = 1;               // limb 维 tile 大小
    uint32_t digit_tile = 1;              // digit 维 tile 大小

    // 三个维度的 tile 个数及总 tile 数。
    uint32_t ct_tiles = 0;
    uint32_t limb_tiles = 0;
    uint32_t digit_tiles = 0;
    uint32_t total_tile_count = 0;

    // 方案评估指标。
    uint64_t estimated_peak_bram_bytes = 0;
    double estimated_total_cost = 0.0;
    TileCostBreakdown cost;
    // 每个 ct tile 的细粒度缓冲统计。
    std::vector<TileBufferUsage> per_tile_buffer_usage;
};

enum class LogicalNodeKind : uint8_t {
    Input,
    KeySource,
    ModUp,
    InnerProd,
    Reduction,
    ModDown,
    Output
};

enum class LogicalTensorRole : uint8_t {
    None,
    CiphertextTile,
    KeyTile,
    AccumTile,
    TempTile
};

struct LogicalEdgePolicy {
    StageConnectionMode preferred_connection = StageConnectionMode::BufferInBRAM;
    std::vector<StageConnectionMode> fallback_order;
    bool allow_shortcut = false;
    bool allow_key_persistent = false;
};

struct LogicalNode {
    uint64_t node_id = 0;
    LogicalNodeKind kind = LogicalNodeKind::Input;
    StageType stage_type = StageType::Dispatch;
    std::vector<uint64_t> depends_on;
    std::vector<LogicalTensorRole> required_inputs;
    LogicalTensorRole produced_output = LogicalTensorRole::None;
    LogicalEdgePolicy edge_policy;
};

struct LogicalGraph {
    bool valid = false;
    std::vector<LogicalNode> nodes;
};

const char* ToString(LogicalNodeKind kind);
const char* ToString(LogicalTensorRole role);
const char* ToString(StageConnectionMode mode);
const char* ToString(StageType stage_type);
void DumpLogicalGraph(const LogicalGraph& graph, std::ostream& os);
std::string FormatLogicalGraph(const LogicalGraph& graph);

// Tile 规划器：在容量约束下搜索并选择低成本 tile 方案。
class TilePlanner {
public:
    // 规划器参数：容量约束、带宽/吞吐、固定开销、加权系数。
    struct Params {
        // BRAM 可用比例（用于从物理容量折算可规划预算）。
        double bram_usable_ratio = 0.95;
        // BRAM 保护区，避免预算用满导致不可调度。
        uint64_t bram_guard_bytes = 512ULL * 1024ULL;
        // temp 缓冲估算比例。
        double temp_buffer_ratio = 0.5;
        // 是否允许 key 常驻策略。
        bool allow_key_persistent = true;

        // 传输与计算性能参数（用于成本模型）。
        uint64_t hbm_to_bram_bw_bytes_per_ns = 32;
        uint64_t bram_to_hbm_bw_bytes_per_ns = 32;
        Time dma_setup_ns = 60;
        Time kernel_launch_ns = 80;
        uint64_t decompose_work_per_ns = 4096;
        uint64_t multiply_work_per_ns = 3072;
        uint64_t basis_work_per_ns = 2048;
        Time per_tile_fixed_overhead_ns = 20;

        // 成本分项权重（总成本 = 各项成本 * 权重 之和）。
        double w_tile_overhead = 1.0;
        double w_key_transfer = 1.0;
        double w_ct_transfer = 1.0;
        double w_output_store = 1.0;
        double w_decompose_compute = 1.0;
        double w_multiply_compute = 1.0;
        double w_basis_convert = 1.0;
    };

    TilePlanner();
    explicit TilePlanner(const Params& params);

    // 输入问题规模，返回最优 tile 方案（若失败则 valid=false）。
    TilePlan Plan(const KeySwitchProblem& problem) const;

private:
    Params params_;
};

// 传输方向枚举（用于 TransferModel 延迟估算）。
enum class TransferDirection : uint8_t {
    HostToHBM,
    HBMToBRAM,
    BRAMToHBM
};

// 传输模型：根据方向、带宽和固定开销估算延迟/能耗。
class TransferModel {
public:
    struct Params {
        uint64_t host_to_hbm_bw_bytes_per_ns = 32;  // Host -> HBM 带宽
        uint64_t hbm_to_bram_bw_bytes_per_ns = 32;  // HBM -> BRAM 带宽
        uint64_t bram_to_hbm_bw_bytes_per_ns = 32;  // BRAM -> HBM 带宽
        Time host_to_hbm_setup_ns = 80;             // Host DMA 固定开销
        Time dma_setup_ns = 60;                     // 片内 DMA 固定开销
        double energy_hbm_byte_nj = 0.0012;         // HBM 每字节能耗（nJ）
    };

    TransferModel();
    explicit TransferModel(const Params& params);

    // 估算指定方向、指定字节量的传输延迟。
    Time EstimateLatency(TransferDirection direction, uint64_t bytes) const;
    // 按字节估算能耗（简化模型）。
    double EstimateEnergyByBytes(uint64_t bytes) const;

private:
    Params params_;
};

// 执行步骤类型：描述 keyswitch DAG 中的节点语义。
enum class TileExecutionStepType : uint8_t {

    // Input / key movement
    KeyLoadHostToHBM,
    KeyLoadHBMToBRAM,
    InputHBMToBRAM,
    KeyHBMToBRAM,

    // Modup subgraph
    ModUpInttTile,
    ModUpBConvTile,
    ModUpNttTile,

    // Inner product
    KSInnerProdTile,

    // Cross-digital reduction
    CrossDigitReduceTile,

    // Explicit spill / reload
    IntermediateBRAMToHBM,
    IntermediateHBMToBRAM,

    // Moddown subgraph
    ModDownInttTile,
    ModDownBConvTile,
    ModDownNttTile,

    // Finalization
    FinalSubtractTile,
    OutputBRAMToHBM,

   // Optional multi-card support
    InterCardSendStep,
    InterCardRecvStep,
    InterCardReduceStep,
    BarrierStep,

    // Legacy single-board compatibility step types.
    // These are kept temporarily so old lowerer/stats paths keep compiling while
    // the new method-aware path is being migrated in.
    DecomposeTile,
    NttTile,
    InttTile,
    BasisConvertTile,
    AccumulateSubtractTile,

    // Legacy multi-card compatibility step types.
    // Keep Merge as the last enumerator so kTileExecutionStepTypeCount remains valid.
    InterCardCommTile,
    InterCardBarrier,
    Merge
};

// StepType 枚举总数（用于数组分配与统计索引）。
inline constexpr std::size_t kTileExecutionStepTypeCount =
    static_cast<std::size_t>(TileExecutionStepType::Merge) + 1;

// StepType -> 下标转换辅助函数。
inline constexpr std::size_t ToIndex(TileExecutionStepType type) {
    return static_cast<std::size_t>(type);
}

// 执行图中的单个步骤节点。
struct TileExecutionStep {
    uint64_t step_id = 0;                                      // 步骤唯一 ID

    TileExecutionStepType type = TileExecutionStepType::InputHBMToBRAM;  // 步骤类型
    StageType stage_type = StageType::Dispatch;                // 所属阶段

    uint32_t ct_tile_index = 0;                                // ct tile 索引
    uint32_t limb_tile_index = 0;                              // limb tile 索引
    uint32_t digit_tile_index = 0;                             // digit tile 索引

    uint32_t ct_idx = 0;
    uint32_t tile_idx = 0;                                     // 兼容字段：tile 索引
    uint32_t digit_idx = 0;                                    // 兼容字段：digit 索引
    uint32_t limb_idx = 0;                                     // 兼容字段：limb 索引

    int32_t src_card = -1;                                     // 源卡（多卡通信用）
    int32_t dst_card = -1;                                     // 目标卡（多卡通信用）

    uint32_t fan_in = 1;                                       // 汇聚输入路数
    uint32_t sync_group = 0;                                   // 同步组 ID
    uint32_t barrier_group = 0;                                // barrier 组 ID

    uint64_t bytes = 0;                                        // 通用字节量（展示/统计）
    uint64_t work_items = 0;                                   // 计算工作量
    uint64_t input_bytes = 0;                                  // 输入字节
    uint64_t output_bytes = 0;                                 // 输出字节

    bool key_hit = false;                                      // 此步是否命中驻留 key
    bool key_persistent = false;                               // key 是否按常驻策略处理

    IntermediateStorageLevel input_storage = IntermediateStorageLevel::BRAM;   // 输入所在层级
    IntermediateStorageLevel output_storage = IntermediateStorageLevel::BRAM;  // 输出所在层级

    bool fused_with_prev = false;                              // 是否与前一步融合
    bool fused_with_next = false;                              // 是否与后一步融合
    bool is_shortcut_path = false;                             // 是否为捷径路径
  
    std::vector<uint64_t> depends_on;                          // 依赖 step_id 列表

    uint64_t output_buffer_id = 0;
    std::vector<uint64_t> input_buffer_ids;
    bool output_can_spill = true;
    bool materialize_output = true;
};

// KeySwitchExecutionModel 的构造参数。
struct KeySwitchExecutionModelParams {
    TilePlanner::Params tile_planner;                      // tile 规划参数
    // 与 U280 片上存储预算保持一致（BRAM36 + URAM288）。
    uint64_t default_bram_capacity_bytes = 44679168ULL;  // 缺省 BRAM 容量
};

// 一次 keyswitch 构建/评估的完整输出。
struct KeySwitchExecution {
    bool valid = false;                                    // 执行结果是否有效
    bool tiled_execution = false;                          // 是否走了 tiled 路径
    bool fallback_used = false;                            // 是否触发 fallback
    KeySwitchFallbackReason fallback_reason = KeySwitchFallbackReason::None;  // fallback 原因
    KeySwitchMethod method = KeySwitchMethod::Poseidon;    // 实际构建所用方法
    KeySwitchMethod requested_method = KeySwitchMethod::Poseidon;  // 请求方法
    KeySwitchMethod effective_method = KeySwitchMethod::Poseidon;  // 解析后有效方法
    bool method_degraded = false;                          // 是否发生方法降级
    KeySwitchFallbackReason degraded_reason = KeySwitchFallbackReason::None;   // 降级原因

    bool key_resident_hit = false;                         // 是否命中驻留 key
    bool key_persistent_bram = false;                      // key 是否在 BRAM 常驻

    uint64_t working_set_bytes = 0;                        // 工作集估算
    uint32_t tile_count = 0;                               // tile 总数

    // 传输统计。
    uint64_t key_host_to_hbm_bytes = 0;
    uint64_t key_hbm_to_bram_bytes = 0;
    uint64_t ct_hbm_to_bram_bytes = 0;
    uint64_t out_bram_to_hbm_bytes = 0;

    // 峰值与成本统计。
    uint64_t peak_bram_bytes = 0;
    PeakBufferUsage peak_buffers;
    TileCostBreakdown tile_cost;
    KeySwitchMethodPolicy policy;
    LogicalGraph logical_graph;

    // 构建细节与步骤轨迹。
    KeySwitchProblem problem;
    TilePlan tile_plan;
    std::vector<TileExecutionStep> steps;

    std::vector<uint64_t> modup_step_ids;
    std::vector<uint64_t> innerprod_step_ids;
    std::vector<uint64_t> reduction_step_ids;
    std::vector<uint64_t> moddown_step_ids;

    // Multi-board metadata.
    MultiBoardMode multi_board_mode = MultiBoardMode::Auto;
    PartitionStrategy partition_strategy = PartitionStrategy::None;
    KeyPlacement key_placement = KeyPlacement::StreamFromHBM;
    CollectiveStrategy collective_strategy = CollectiveStrategy::None;
    uint32_t active_cards = 1;

    // 汇总预测指标。
    uint64_t predicted_hbm_bytes = 0;
};

// Keyswitch 执行模型：负责问题建模、tile 规划、步骤 DAG 构建与结果汇总。
class KeySwitchExecutionModel {
public:
    KeySwitchExecutionModel();
    explicit KeySwitchExecutionModel(
        const KeySwitchExecutionModelParams& params);

    // 将请求转换成标准化问题规模（供 planner 与 builder 使用）。
    KeySwitchProblem BuildProblem(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    // 按方法分发构建入口。
    // 支持：
    // - Poseidon / OLA / FAB / FAST / HERA（共用单板路径）
    // - Cinnamon
    // 不支持的方法会返回显式 UnsupportedMethod fallback。
    KeySwitchExecution Build(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    // 单板 keyswitch 构建路径。
    // 由 Poseidon / OLA / FAB / FAST / HERA 共用。
    KeySwitchExecution BuildSingleBoard(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state) const;

    TilePlanner::Params TilePlannerParams() const;

private:
    // 构建过程上下文：维护 step_id 与 HBM 传输累计。
    struct BuildContext {
        KeySwitchExecution* execution = nullptr;
        uint64_t next_step_id = 1;

        uint64_t hbm_read_bytes = 0;
        uint64_t hbm_write_bytes = 0;
        uint64_t hbm_round_trips = 0;
    };

    TileExecutionStep MakeStep(
        TileExecutionStepType type,
        StageType stage_type,
        uint32_t ct_idx, 
        uint32_t limb_idx, 
        uint32_t digit_idx, 
        uint64_t input_bytes, 
        uint64_t output_bytes, 
        uint64_t work_items, 
        IntermediateStorageLevel input_storage, 
        IntermediateStorageLevel output_storage
    ) const;

    // 追加单个 step，并更新内存占用快照与峰值统计。
    uint64_t AppendStep(
        BuildContext* ctx,
        TileExecutionStep step) const;

    void AddDependency(
        BuildContext* ctx,
        uint64_t step_id,
        uint64_t dep_step_id
    ) const;
    
    void AddDependencies(
        BuildContext* ctx,
        uint64_t step_id,
        const std::vector<uint64_t>& dep_step_ids
    ) const;

    void MarkSubgraphSteps(
        KeySwitchExecution* execution,
        const std::vector<uint64_t>& step_ids,
        StageType coarse_stage
    ) const;

    // 构建 ModUp 子图，返回该子图各步骤 ID（按执行顺序）。
    std::vector<uint64_t> BuildModUpSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        uint64_t ct_chunk_bytes,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps
    ) const;

    // 构建 InnerProd 子图，返回该子图各步骤 ID。
    std::vector<uint64_t> BuildInnerProdSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        uint64_t key_chunk_bytes,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps
    ) const;

    // 构建 Reduction 子图，返回该子图各步骤 ID。
    std::vector<uint64_t> BuildReductionSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps
    ) const;

    // 构建 ModDown 子图，返回该子图各步骤 ID。
    std::vector<uint64_t> BuildModDownSubgraph(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint64_t out_bytes,
        uint64_t work_items,
        const KeySwitchMethodPolicy& policy,
        const std::vector<uint64_t>& deps) const;

    // 按策略连接两个子图（可直连或插入中间缓冲步骤）。
    std::vector<uint64_t> ConnectSubgraphsWithPolicy(
        BuildContext* ctx,
        uint32_t ct_idx,
        uint32_t limb_idx,
        uint32_t digit_idx,
        StageConnectionMode mode,
        uint64_t bytes,
        const std::vector<uint64_t>& deps
    ) const;

    // 构建完成后的统一收尾与汇总。
    void FinalizeExecution(
        BuildContext* ctx
    ) const;

    // 方法分发辅助：
    // 支持的方法返回完整 execution；未实现方法返回显式 unsupported fallback。
    KeySwitchExecution BuildByMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod resolved_method
    ) const;

    // 判定请求 key 是否在参与执行的卡上驻留命中。
    bool ResidentKeyHit(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state
    ) const;

    // 返回“不支持方法”的 execution 结果。
    KeySwitchExecution BuildUnsupportedMethod(
        KeySwitchMethod requested_method,
        KeySwitchMethod effective_method
    ) const;

    // 共享单板方法构建入口（Poseidon/OLA/FAB/FAST/HERA）。
    KeySwitchExecution BuildSharedSingleBoardMethod(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod method) const;

    KeySwitchExecution BuildSharedSingleBoardPhysical(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod method) const;

    LogicalGraph BuildSharedSingleBoardLogicalGraph(
        const KeySwitchMethodPolicy& policy) const;

    // Cinnamon 专用构建入口。
    KeySwitchExecution BuildCinnamon(
        const Request& req,
        const ExecutionPlan& plan,
        const SystemState& state,
        KeySwitchMethod method) const;

private:
    KeySwitchExecutionModelParams params_;
    TilePlanner planner_;
};
