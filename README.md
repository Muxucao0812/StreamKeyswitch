# KeyAwareSwitch

面向同态加密（HE）`keyswitch` 资源调度与执行代价研究的 C++17 离散事件模拟器。

## 当前定位

当前主线以 `cycle_stub` 后端为核心，已支持：
- 方法感知的执行建模（不同 `--ks-method` 生成不同执行图与运行计划）。
- 资源感知的 cycle 级指令/争用模型（`ComputeArray` / `SPU` / `HBM` / `Interconnect`）。
- 多板执行模式与跨板通信代价可见化（send/recv/reduce）。
- 从请求到运行计划的可观测性（`--dump-logical-graph`、`--dump-runtime-plan`）。

> 说明：CLI 中 `--backend cycle` 与 `--backend cycle_stub` 当前都指向同一实现。

## 核心能力概览

### 1) 调度器（Scheduler）
支持：
`fifo` / `affinity` / `static_partition` / `score` / `pool` / `hierarchical_a` / `hierarchical_b` / `hierarchical_c` / `hierarchical_d`（`hierarchical` 为别名）

### 2) 后端（Backend）
支持：
`cycle` / `cycle_stub`

### 3) 工作负载（Workload）
支持：
`synthetic` / `burst`

### 4) Keyswitch 方法（Method）
支持：
`poseidon` / `fab` / `fast` / `ola` / `hera` / `cinnamon`

这些方法通过策略层影响：
- 单板逻辑图构型与依赖关系。
- Runtime Planner 的 step DAG（如 reduce/moddown 连接方式、流水化与 barrier 行为）。
- 多板路径下的通信与聚合行为。

## 新增建模点（近期更新）

### NTT / INTT / BConv 细粒度化
`cycle` lowering 不再把这些阶段当作单个黑盒延迟，而是拆分为子阶段指令链：
- NTT/INTT：`Load`、`ButterflyLocal`、`Transpose1`、`ButterflyGlobal`、`Transpose2`、`Store`
- BConv：`Load`、多次 `MAC pass`、`Reduce`、`Store`

### 资源类争用模型
所有指令映射到统一资源类做争用模拟：
- `ComputeArray`：算术密集阶段（含 `Decompose`）
- `SPU`：转置/重排类操作
- `HBM`：片外访存阶段
- `Interconnect`：跨板通信

### 跨板通信显式建模
新增并打通以下指令到 cycle 模型：
- `InterCardSend`
- `InterCardRecv`
- `InterCardReduce`

通信延迟由硬件参数驱动（链路数、带宽、启动开销、流水深度等），并与其他通信指令发生同类资源争用。

## 编译

```bash
cmake -S . -B build-cmake
cmake --build build-cmake -j4
```

## 常用运行示例

### 1) 基线：FIFO + cycle_stub + synthetic
```bash
./build-cmake/keyaware_sim --scheduler fifo --backend cycle_stub --workload synthetic
```

### 2) 方法切换：FAST
```bash
./build-cmake/keyaware_sim --scheduler affinity --backend cycle_stub --workload synthetic \
  --ks-method fast
```

### 3) 跨板路径：Cinnamon + 多卡
```bash
./build-cmake/keyaware_sim --scheduler pool --backend cycle_stub --workload burst \
  --num-cards 4 --enable-multi-card --ks-method cinnamon
```

### 4) 观测执行图
```bash
./build-cmake/keyaware_sim --scheduler fifo --backend cycle_stub --workload synthetic \
  --ks-method poseidon --dump-logical-graph --dump-runtime-plan
```

### 5) 使用外部 HE 参数
```bash
./build-cmake/keyaware_sim --scheduler fifo --backend cycle_stub --workload synthetic \
  --he-params he_params/ckks_32k.cfg
```

## 测试

```bash
ctest --test-dir build-cmake --output-on-failure
```

如需单测二进制，也可直接运行：
- `./build-cmake/keyaware_cycle_tests`
- `./build-cmake/keyaware_runtime_plan_tests`
- `./build-cmake/keyaware_unit_tests`
- `./build-cmake/keyaware_regression_tests`

## 常用参数

- `--scheduler <name>`：调度策略
- `--backend <cycle|cycle_stub>`：后端
- `--workload <synthetic|burst>`：负载类型
- `--ks-method <poseidon|fab|fast|ola|hera|cinnamon>`：keyswitch 方法
- `--num-cards <uint32>`：卡数
- `--enable-multi-card` / `--disable-multi-card`：是否允许多卡请求
- `--he-params <path>`：HE 参数文件
- `--csv-output <path>`：导出运行指标
- `--dump-logical-graph`：打印逻辑图
- `--dump-runtime-plan`：打印运行计划图
- `--help`：查看完整参数说明

## HE 参数文件格式

使用 `key=value` 文本格式，支持 `#` 注释。

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

## 后续方向

- 继续提升 cycle 模型精度（更细粒度流水/重叠、更多硬件约束）。
- 将调度器决策更紧密地连接到真实执行代价统计（含跨板通信成本）。
- 补强复杂配置组合下的自动化回归覆盖。
