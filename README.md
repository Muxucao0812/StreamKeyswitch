首先，对于多user的keyswitch的操作，要进行key的stationary，因为key的数据量是最大的。
对于整个系统的模拟要分为5层。
1. Workload layer：负责生成和管理请求
(1) User
(2) Request
(3) Arrival Time
(4) Keyswitch Profile
2. System State Layer: 负责系统资源的状态
(1) cards
(2) resident key
(3) memory occupancy
(4) links
(5) pools
3. Scheduler Layer： 负责决策
(1) 请求放哪张卡
(2) 是否扩到多卡
(3) 是否按 user affinity
(4) 走树形资源分配
4. Execution Backend Layer: 负责估算执行时间
(1) analytical backend
(2) table backend
(3) cycle backend

5. Simulation Core: 负责事件推进
(1) arrival
(2) dispatch
(3) start
(4) finish
(5) load done
(6) merge done