# KeyAwareSwitch

一个面向同态加密（HE）`keyswitch` 资源分配研究的 C++17 离散事件模拟器。

## 项目目标
- 多用户请求到达与排队
- 多 accelerator card 资源调度
- key/context 驻留与切换开销建模
- 对比不同调度策略对 latency / throughput / fairness / reload 的影响
- 支持从 task-level 逐步扩展到 stage-level、primitive-level（再到未来 cycle-level）

## 当前状态（已完成 / 未完成）

### 已完成（Step 1 ~ Step 16）
- [x] Step 1: 工程骨架（CMake、核心模型、Scheduler、Simulator、Metrics、main）
- [x] Step 2: Workload 与 UserProfile 增强（synthetic / burst，用户差异化参数）
- [x] Step 3: Metrics 扩展（request / user / card / system 四层）
- [x] Step 4: AnalyticalBackend 升级为 stage-level
- [x] Step 5: 新增 StaticPartitionScheduler / ScoreScheduler
- [x] Step 6: multi-card request（k=1/2/4）与扩卡模型
- [x] Step 7: ResourcePool + PoolScheduler（两级调度）
- [x] Step 8: 固定树 HierarchicalScheduler（A/B/C/D）
- [x] Step 9: TableBackend（查表 + nearest/interpolation + fallback）
- [x] Step 10: 可复现实验基础（deterministic event ordering、严格 CLI、seed control、ExperimentConfig）
- [x] Step 11: pool/tree 外部配置加载，HierarchicalScheduler 消费外部树
- [x] Step 12: 测试与回归体系（ctest + unit/regression）
- [x] Step 13: 外部 profiling CSV 驱动的 TableBackend
- [x] Step 14: 批量实验编排与 CSV 导出（`scripts/batch_runner.py`）
- [x] Step 15: 层次化资源树自动搜索（hill-climbing，树导出可回灌）
- [x] Step 16: CycleBackend stub / Hybrid backend（primitive trace + stub simulator）

### 未完成（后续方向）
- [ ] 真实 cycle-accurate backend（当前仅 `cycle_stub`）
- [ ] primitive DAG overlap / pipeline 建模（当前 primitive 近似串行）
- [ ] link-level / network-level 细粒度仿真
- [ ] 基于真实硬件 profiling 的能耗与显存校准（目前仍为简化模型）
- [ ] 更强的树搜索算法（beam/annealing/并行评估）
- [ ] 更高覆盖率自动化测试（尤其是复杂配置组合与边界场景）

## 当前可用运行组合

### Scheduler
`fifo` / `affinity` / `static_partition` / `score` / `pool` / `hierarchical_a` / `hierarchical_b` / `hierarchical_c` / `hierarchical_d`

### Backend
`analytical` / `table` / `cycle_stub` / `hybrid`

### Workload
`synthetic` / `burst`

### HE 参数配置
`he_params/` 目录下的配置文件可作为 workload 的参数来源，用来统一驱动：
- `UserProfile.key_bytes`
- `UserProfile.key_load_time`
- `KeySwitchProfile.num_polys`
- `KeySwitchProfile.num_digits`
- `KeySwitchProfile.num_rns_limbs`

默认使用内建参数；如果指定 `--he-params <path>`，则从外部文件加载。

## 方法总览（所有方法）

### 1. Scheduler 方法

#### `fifo`
- 核心逻辑：严格按到达顺序（队首优先）调度。
- 资源选择：从当前空闲卡中按顺序选卡。
- 特点：最容易解释与对照，但可能出现队首阻塞（Head-of-Line Blocking）。
- 适用：做 baseline、验证系统稳定性。

#### `affinity`
- 核心逻辑：扫描队列，优先挑选可以利用 resident key 的请求。
- 资源选择：优先 `resident_user == req.user_id` 的卡，不够再用其他空闲卡。
- 特点：通常 reload 更少、吞吐更高；但不严格先来先服务。
- 适用：关注 key 命中率、reload 成本时。

#### `static_partition`
- 核心逻辑：用户固定映射到 pool（`user_id % num_pools`）。
- 资源约束：请求只能在所属 pool 卡内调度。
- 特点：隔离性强；但可能出现 pool 间负载不均。
- 适用：多租户隔离实验。

#### `score`
- 核心逻辑：按评分函数选“请求-卡”组合。
- 评分因素：`waiting_time`、`switch_cost`、`queue_pressure`、`priority`。
- 特点：灵活可调；但依赖权重设定，参数敏感。
- 适用：策略权衡/参数扫描实验。

#### `pool`
- 核心逻辑：两级策略。
- 先选 pool：根据 `latency_sensitive` 选择 latency/batch pool。
- 再选卡：池内做 affinity 优先。
- 特点：兼顾隔离与命中率。
- 适用：低时延流量与批处理流量并存场景。

#### `hierarchical_a`
- 核心逻辑：固定树 A（全共享）。
- 语义：所有用户共享一个叶子资源域。
- 特点：接近共享集群基线。

#### `hierarchical_b`
- 核心逻辑：固定树 B（按 latency-sensitive 分两池）。
- 语义：latency 请求与 batch 请求走不同叶子。
- 特点：优先保证低时延流量。

#### `hierarchical_c`
- 核心逻辑：固定树 C（按 user hash 固定叶子）。
- 语义：用户稳定映射到子树/叶子。
- 特点：强隔离，跨用户干扰更小。

#### `hierarchical_d`
- 核心逻辑：固定树 D（两池 + 池内 affinity）。
- 语义：先按 latency 类分池，再在叶子内部做 resident user 优先。
- 特点：是当前层次化策略里最常用的综合方案。

### 2. Backend 方法

#### `analytical`
- 核心逻辑：stage-level 公式估时。
- stage：`KeyLoad -> Dispatch -> Decompose -> Multiply -> BasisConvert -> Merge(多卡时)`。
- 特点：速度快、可解释性好，适合大规模 sweep。

#### `table`
- 核心逻辑：从 profile 表查 stage 延迟。
- 维度：stage、digits、rns limbs、card count、key hit/miss 等。
- 查询策略：`exact -> interpolated/nearest -> analytical fallback`。
- 特点：比纯公式更贴近 profile 数据。

#### `cycle_stub`
- 核心逻辑：把 stage 转成 primitive trace，调用 primitive simulator stub。
- 输出：primitive 聚合后的 latency/energy/memory。
- 特点：为未来 cycle-level 预留接口，当前仍是简化 stub。

#### `hybrid`
- 核心逻辑：混合模式。
- 默认策略：关键 stage（如 `Multiply`、`Merge`）走 primitive stub，其余 stage 走 coarse backend。
- `--hybrid-coarse`：可选 `analytical` 或 `table`。
- 特点：可平滑从粗粒度模型迁移到细粒度模型。

### 3. Workload 方法

#### `synthetic`
- 结构：多用户均匀交织请求。
- 可调参数：`--num-users`、`--requests-per-user`、`--inter-arrival`、`--synthetic-start-time`。
- HE 参数联动：request 的基础 digits / RNS limbs / key size 由 HE 参数文件给出，再叠加小范围扰动。
- 适用：稳态吞吐和平均延迟对比。

#### `burst`
- 结构：突发到达模型。
- 可调参数：`--bursts`、`--requests-per-user-per-burst`、`--intra-burst-gap`、`--inter-burst-gap`、`--burst-level`。
- HE 参数联动：与 `synthetic` 相同，request 结构由统一 HE 参数文件驱动。
- 适用：高峰期延迟和队列压力测试。

### 4. 运行与实验方法

#### 单次运行
- 直接设置 scheduler/backend/workload + seed。
- 适合排查策略行为、debug 单组实验。

#### 批量运行（Step 14）
- 使用 `scripts/batch_runner.py` 做 sweep。
- 支持输出统一 CSV，便于画图和论文复现。

#### 树搜索（Step 15）
- 通过 `--search-tree` 自动优化 resource tree。
- 输出的优化树可再次作为 `--tree-config` 输入回灌。

#### 测试与回归（Step 12）
- `ctest` 跑 unit + regression。
- 保证新增改动不破坏已有行为。

## 编译
```bash
cmake -S . -B build-cmake
cmake --build build-cmake -j4
```

## 常用运行示例

### 1) FIFO + Analytical + Synthetic（baseline）
```bash
./build-cmake/keyaware_sim --scheduler fifo --backend analytical --workload synthetic
```

### 2) Affinity + Table + Burst（看 reload 与尾延迟）
```bash
./build-cmake/keyaware_sim --scheduler affinity --backend table --workload burst \
  --profile-table profiles/example_stage_profile.csv
```

### 2.1) 使用外部 HE 参数文件
```bash
./build-cmake/keyaware_sim --scheduler fifo --backend analytical --workload synthetic \
  --he-params he_params/ckks_32k.cfg
```

作用：
- 不再在代码里手改 key 大小或 digits。
- 通过 `he_params/ckks_32k.cfg` 统一驱动 workload 中和同态参数相关的字段。
- 方便后续比较不同 HE 参数集对 latency / reload / throughput 的影响。

### 3) Cycle Stub（primitive 接口链路）
```bash
./build-cmake/keyaware_sim --scheduler fifo --backend cycle_stub --workload synthetic
```

### 4) Hybrid（关键 stage 细化）
```bash
./build-cmake/keyaware_sim --scheduler affinity --backend hybrid --hybrid-coarse table --workload burst
```

### 5) 层次树搜索 + 回灌
```bash
./build-cmake/keyaware_sim --scheduler hierarchical_d --backend analytical --workload synthetic \
  --search-tree --search-steps 20 --search-neighbors 8 --search-output-tree trees/optimized.cfg
```

## 批量实验（Step 14）
```bash
python3 scripts/batch_runner.py \
  --bin ./build-cmake/keyaware_sim \
  --output-csv results/step14.csv \
  --template latency_vs_scheduler
```

## 测试与回归（Step 12）
```bash
ctest --test-dir build-cmake --output-on-failure
```

或：
```bash
./scripts/run_regression.sh
```

## 关键配置入口
- pool 配置：`--pool-config <path>`
- tree 配置：`--tree-config <path>`
- profile table：`--profile-table <path>`
- HE 参数文件：`--he-params <path>`
- hybrid coarse backend：`--hybrid-coarse <analytical|table>`
- seed 控制：`--seed <uint64>`
- CSV 导出：`--csv-output <path>`
- CLI 帮助：`--help`

## HE 参数文件格式
当前使用简单的 `key=value` 文本格式，支持 `#` 注释。

示例：
```txt
# he_params/ckks_16k.cfg
name = ckks_16k
poly_modulus_degree = 16384
num_digits = 3
num_rns_limbs = 4
num_polys = 2
key_component_count = 2
bytes_per_coeff = 8
key_storage_divisor = 128
key_load_base_time = 144
key_load_bandwidth_bytes_per_ns = 32
```

字段说明：
- `poly_modulus_degree`：多项式模次数
- `num_digits`：keyswitch 分解 digit 数
- `num_rns_limbs`：RNS limb 数
- `num_polys`：参与计算的多项式数
- `key_component_count`：key 中的组件数量
- `bytes_per_coeff`：每个系数的字节数
- `key_storage_divisor`：把理论 key 大小缩放到当前模拟器使用的存储量级
- `key_load_base_time`：固定 key load 开销
- `key_load_bandwidth_bytes_per_ns`：按带宽折算出来的附加 load 时间

## 备注
- 当前默认目标是研究策略比较与系统趋势，不是硬件周期级精确复现。
- `cycle_stub` / `hybrid` 的定位是为后续真实细粒度模型预留架构接口。
