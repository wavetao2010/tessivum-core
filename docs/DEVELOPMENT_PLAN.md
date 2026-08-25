# Tessivum Core 开发计划

> 状态：阶段一实施基线  
> 项目：`tessivum-core`  
> 目标：独立、可发布的 Rust 时空组合运行时  
> 基线日期：2026-08-17

## 1. 文档职责

本文是 `tessivum-core` 仓库的阶段一权威开发计划，负责：

- 项目边界；
- 实施顺序；
- 每个里程碑的交付物和验收条件；
- 公开接口冻结门槛；
- Cordis 行为兼容基线；
- Native、Extism/WASM、Legacy Node 三种插件运行时；
- 测试、性能、安全和发布要求。

Tessivum 产品、Agent、Session、LLM、Tools、CLI、Host/API 和 Web 不属于本仓库；它们由独立的 `tessivum` 仓库实现，并只依赖本仓库公开接口。

## 2. 项目目标

Tessivum Core 重建 Cordis 的核心语义，而不是逐行翻译 TypeScript：

1. Context 与父子 Scope；
2. 可逆、可等待、可诊断的插件生命周期；
3. Service 注册、依赖门控、隔离和替换；
4. emit、parallel、serial、bail、waterfall 事件模式；
5. 声明式 Loader、配置树和事务更新；
6. 同进程 Native Rust 插件；
7. Extism/WASM 沙箱插件；
8. 现有 npm/Cordis 插件的 Legacy Node 兼容运行时。

最终依赖方向固定：

```text
tessivum product
       ↓
tessivum-core public API
```

`tessivum-core` 不得依赖或认识 Agent、Session、Tool、Prompt、LLM 等产品领域类型。

## 3. 源码与行为基线

固定参考版本：

| 来源 | 提交 |
|---|---|
| [Cordis](https://github.com/cordiverse/cordis) | `47f943859bef60e4160492346772ded9b24f765a` |
| [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) | `8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4` |
| Harness vendored Cordis 基线 | `56b3d4f725681cf4556c1a8695a709cc3b6eed74` |

必须纳入 Harness [`vendor/README.md`](https://github.com/deepseek-ai/deepseek-harness/blob/8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4/vendor/README.md) 中记录的本地修复，尤其是：

- reentrant disposal；
- 初始化失败回滚；
- 异步 cleanup 静默；
- unload 期间禁止逃逸式 effect；
- 父子 Fiber 释放顺序；
- lazy config resolution；
- Loader/Include/Group 事务更新与回滚。

发生冲突时，行为优先级：

```text
DeepSeek Harness 当前可观察行为
  > Harness vendored 补丁契约
  > 当前 Cordis 上游行为
  > TypeScript 私有实现细节
```

## 4. 明确不做

- 不复制 JavaScript Proxy、原型链、`this` 绑定或 TypeScript 声明合并。
- 不把 Rust trait object 或引用穿过 WASM/Node 边界。
- 不让 Extism 承担高频内部 Token 流和核心调度。
- 不宣称 Extism JS PDK 等同 Node.js。
- 不在 Rust Host 中执行无约束 JavaScript 配置表达式。
- 不把 Legacy Node Bridge 宣称为安全沙箱。
- 不在没有真实基准前承诺固定内存或并发数字。
- 不创建 Agent/Harness 专属服务代理；通用 Bridge transport 在本仓库，领域代理归产品仓库。
- 不为未来可能出现的运行时提前创建抽象层。

## 5. 阶段一完成定义

首个供 Tessivum 产品固定依赖的版本必须满足：

- Context、Scope、Fiber、Service 和 Event Bus 公开接口可用；
- 生命周期契约夹具通过；
- required service 增删会正确驱动 consumer Pending/Active/Unloading；
- isolate realm 行为正确；
- Loader 能解析、组合、更新并回滚配置树；
- Native Plugin 端到端运行；
- WASM Plugin 端到端运行并受权限与资源上限约束；
- Legacy Node Plugin 无源码修改运行，并在进程崩溃后清理全部代理资源；
- 公开错误码和协议已版本化；
- 性能基线已测量并保存；
- 没有未拥有的后台任务、监听器、服务或 Handle；
- Tessivum 产品只通过公开接口完成一个最小 Headless 探针。

## 6. 仓库结构

只按真实编译目标和进程边界拆分：

```text
tessivum-core/
├── Cargo.toml
├── crates/
│   ├── tessivum-core/        # Context、Scope、Fiber、Service、Events、Loader
│   ├── tessivum-extism/      # Extism Host adapter
│   ├── tessivum-pdk/         # Rust/WASM Guest SDK
│   └── tessivum-node-bridge/ # Rust 侧 Legacy Node transport
├── node/
│   └── compat-host/          # 原版 Cordis + npm 插件执行进程
├── fixtures/
│   └── conformance/          # 与 TypeScript oracle 共用的行为夹具
├── examples/
│   ├── native-minimal/
│   ├── wasm-minimal/
│   └── legacy-node-minimal/
└── docs/
    └── DEVELOPMENT_PLAN.md
```

初期允许 `crates/tessivum-core` 内包含普通 module。只有编译目标、依赖隔离或独立发布确实要求时才继续拆 crate。

## 7. 核心不变量

实现和评审始终以这些不变量为准。

### 7.1 生命周期

1. 每个可释放资源只有一个 owner Scope。
2. Scope 进入 Unloading 后拒绝新 effect、listener、service 和 child Fiber。
3. `dispose()` 幂等；重复调用加入同一清理过程。
4. 父 Scope 的 dispose 在全部子 Scope 和异步 cleanup 静默后才完成。
5. 初始化失败撤销初始化过程中已经创建的全部资源。
6. 多个 cleanup 失败时继续清理其余资源，最终返回聚合错误。
7. Rust `Drop` 不能代替异步 `dispose().await`。

### 7.2 服务

1. required service 缺失时插件不得部分启动。
2. provider 消失后旧实现或 Handle 不得继续调用。
3. provider 恢复时 consumer 根据实现 generation 重新激活。
4. isolate realm 的同名服务互不可见，除非显式共享 realm label。
5. optional service 查询不能改变 Fiber 激活状态。

### 7.3 事件

1. listener 与 owner Scope 绑定，卸载后不可调度。
2. 分发顺序稳定且与注册顺序一致，除非显式 prepend。
3. waterfall 只有调用 `next` 才继续下游。
4. 跨运行时 continuation 一次性、有取消和截止时间。
5. 单个 WASM/Node 插件实例默认串行处理回调，避免无意重入。

### 7.4 跨运行时

1. 只传值、消息、不可伪造 Handle，不传内存引用或对象身份。
2. 每个 Handle 携带 owner generation；连接或实例失效后立即拒绝。
3. 每条队列、payload、调用时间、实例内存和并发都有上限。
4. Node/WASM 崩溃不能污染 Native Registry。
5. 协议错误不得降级为无检查调用。

### 7.5 Loader

1. 配置行顺序不决定依赖激活顺序。
2. 候选配置先在 detached 状态校验。
3. 更新失败保留最后可运行树。
4. rollback 失败必须大声报告聚合错误，不得伪装成功。
5. runtime 类型必须显式或按确定规则推导。

---

# 8. 实施里程碑

## M0：行为基线与差分夹具

### 目标

先定义“正确行为”，再写框架。

### 工作项

- 盘点 vendored Cordis 的 Context、Registry、Fiber、Reflect、Events、Service 测试；
- 盘点 Harness lifecycle 和 Loader 本地补丁；
- 建立语言无关 fixture schema；
- 建立 TypeScript oracle runner；
- 产出标准化状态轨迹；
- 固定可比较字段和需归一化字段；
- 建立有意差异登记表。

### 轨迹至少记录

```text
fiber-created
fiber-state-changed
service-provided
service-removed
listener-added
listener-removed
event-dispatched
effect-created
effect-disposed
plugin-error
config-committed
config-rolled-back
```

每条记录包含稳定的 fixture-local ID，不使用实现内存地址或随机 Fiber ID。

### 验收

- 同一 fixture 重复运行得到同一归一化轨迹；
- 覆盖生命周期、服务、隔离、事件和 Loader；
- oracle runner 的失败明确定位 fixture 与步骤；
- Rust runner 入口已经定义但不要求实现。

### 出口

M0 未完成前，不开始完整框架脚手架或 Extism 集成。

## M1：Scope、Fiber 与 Effect 生命周期

### 目标

建立可证明正确的资源所有权与异步释放内核。

### 工作项

- `ScopeId`、`FiberId` 和 generation；
- 父子 Scope 树；
- Fiber 状态机；
- effect/resource registry；
- 同步/异步 disposer；
- child Fiber ownership；
- 初始化事务；
- 取消 token；
- quiescence 等待；
- 错误聚合；
- effect 诊断树。

### 必测场景

- 正常 start/dispose；
- 重复 dispose；
- dispose 进行中再次 dispose；
- start 内触发自身/父级 dispose；
- start 同步失败；
- start 异步失败；
- cleanup 同步/异步失败；
- child 先于 parent；
- parent 等待 child；
- unload 中尝试注册资源；
- 取消与完成竞争。

### 验收

- M0 生命周期轨迹通过；
- 测试结束后 resource registry 为空；
- 所有后台任务均能归属和静默；
- Miri/loom 或适合的并发检查覆盖关键状态转换；具体工具以实际可用性为准。

## M2：Context、Service Registry 与依赖门控

### 目标

实现 Cordis 的空间组合语义。

### 工作项

- 轻量 `ContextHandle`；
- service key 与 contract version；
- Native service provider；
- required/optional dependency；
- Pending/Active 自动转换；
- provider replacement generation；
- isolate realm；
- shared realm label；
- intercept config chain；
- service 可用性通知；
- 诊断快照。

### 设计约束

- Native typed service 和跨运行时 dynamic service 分离；
- 公开诊断使用稳定字符串键，Rust 内部可使用 trait/generic；
- consumer 不持有绕过 generation 检查的长期裸引用；
- 服务通知批处理不得改变可观察提交顺序。

### 验收

- provider/consumer 加载顺序无关；
- provider 删除使 consumer 完整卸载；
- provider 恢复使 consumer 重新加载；
- provider 替换不会调用旧实现；
- isolate/shared realm fixture 通过；
- optional service 缺失不阻塞插件。

## M3：Event Bus

### 目标

实现五种事件组合方式及生命周期绑定。

### 工作项

- typed Native event；
- dynamic serialized event；
- emit；
- parallel；
- serial；
- bail；
- waterfall；
- prepend/global/scoped filter；
- listener ResourceId；
- 错误与 AggregateError 等价模型；
- dispatch 诊断和慢 listener 观测。

### 验收

- 注册顺序、prepend 和 scope filter 正确；
- serial/bail 的有效结果判断与基线一致；
- parallel 等待全部 listener 并聚合错误；
- waterfall 支持包装、修改、短路和返回值传播；
- listener owner 卸载后不再执行；
- dispatch 中卸载 listener 的行为已定义并测试。

## M4：Loader、Entry Tree 与事务更新

### 目标

把声明式配置稳定映射到插件运行时。

### 工作项

- Entry/Group/Tree 数据模型；
- YAML/JSON 解析；
- stable entry id；
- config schema validation；
- inject/isolate/intercept/disabled；
- profile/bundle patch 基础算法；
- runtime resolution；
- detached candidate；
- diff、apply、commit、rollback；
- 原子持久化；
- import/package resolver 接口；
- HMR driver 接口位置。

### 配置表达式

第一版提供显式、可审计表达式：

- 环境变量白名单；
- platform/architecture；
- profile variable；
- 已声明服务的只读配置投影；
- 字符串/布尔/空值运算的有限集合。

无法安全表达的旧 `!!js` 子树交给 Legacy Node Loader。禁止在 Rust 主进程中加入通用 JS eval。

### 验收

- base/headless 风格 Entry Tree 能解析；
- patch 层优先级和整行 config 替换正确；
- 重复 id 和无效 runtime 装载前失败；
- 更新候选失败保留旧树；
- replace 失败恢复旧插件；
- rollback 本身失败产生完整聚合诊断；
- 持久化使用原子替换。

## M5：Native Plugin API

### 目标

为 Tessivum 产品提供最短、明确、类型安全的同进程插件接口。

### 工作项

- Plugin descriptor；
- config validation；
- instantiate/start/update/dispose；
- Context service/event/effect API；
- plugin snapshot；
- minimal native example；
- API 文档和编译示例。

### API 原则

- 插件生命周期只有一个权威 owner；
- 不为一个实现创建多余 factory/interface；
- 不暴露 Registry/Fiber 私有可变字段；
- 配置和错误使用明确类型；
- 热路径不经过 JSON；
- 公开 API 的泛型复杂度必须由实际类型安全收益证明。

### 验收

Native 示例完成：

```text
provider start
→ consumer dependency satisfied
→ consumer start
→ event dispatch
→ provider replacement
→ consumer reload
→ root dispose
→ zero resources
```

## M6：Extism/WASM ABI 与 PDK

### 目标

建立版本化、受权限约束的跨语言插件路径。

### 工作项

- `cordis.plugin/v1` manifest；
- Guest exports：init/call/event/update/stop；
- JSON request/response envelope；
- stable error envelope；
- Host Functions capability registry；
- permissions；
- memory/time/fuel/concurrency/output limits；
- cancellation；
- instance lifecycle；
- Rust PDK；
- TypeScript/JavaScript 示例；
- ABI compatibility tests。

### 第一版 Host Functions

只实现跨插件通用能力：

```text
cordis.log
cordis.config.get
cordis.service.call
cordis.event.emit
cordis.event.subscribe
cordis.registration.dispose
cordis.kv.get
cordis.kv.set
```

文件、HTTP、数据库等领域能力由 Host 注册独立 capability；Core 不内置产品策略。

### 验收

- Rust Guest 和 JS/TS Guest 均可装载；
- Guest 注册并释放一个资源；
- Guest 调用一个允许的 Host Function；
- 未授权调用稳定失败；
- event callback 和 config update 生效；
- timeout/trap/内存超限不影响其他插件；
- stop 后所有 Handle 失效；
- ABI 不匹配在执行 Guest 代码前失败。

## M7：Legacy Node Bridge

### 目标

原样运行现有 npm/Cordis 插件，并将通用生命周期映射到 Rust Core。

### Rust transport 工作项

- 长度前缀 frame；
- protocol version；
- connection generation；
- request/response/error/cancel；
- 有界发送/接收队列；
- payload 上限；
- heartbeat/exit handshake；
- process supervision；
- generation-wide cleanup；
- generic service/event/registration messages。

### Node compat-host 工作项

- vendored `@deepseek-ai/cordis`；
- npm/package resolver；
- function/object/class plugin；
- Loader；
- Fiber 状态投影；
- service proxy registry；
- event callback registry；
- async disposer settlement；
- 日志独立 frame；
- graceful shutdown。

### 明确边界

Core Bridge 只拥有通用 transport 和 lifecycle。`tools`、`agents`、`sessions` 等领域代理由 Tessivum 产品实现。

### 验收

无需改源码运行：

- function plugin；
- Service subclass；
- required inject；
- event listener；
- waterfall listener；
- async disposer；
- 使用普通 Node API 的插件。

故障验收：

- Node 进程崩溃后所有 generation-owned 注册被删除；
- Native consumer 因服务消失转 Pending；
- 重启后不复用旧 Handle；
- 超大 frame、超时、协议版本错误和日志污染均明确失败；
- Bridge 退出无孤儿进程。

## M8：硬化、基准与首个发布

### 工作项

- 完整 conformance suite；
- 差分报告；
- API 文档；
- ABI/Bridge 协议文档；
- 示例；
- 性能基准；
- 安全复核；
- release notes；
- Tessivum 最小 Headless 集成探针；
- 版本与兼容策略。

### 发布门槛

- M0–M7 验收全部通过；
- 无未记录行为差异；
- 无已知资源泄漏或关闭竞态；
- panic/trap/Node crash 不破坏其他运行时；
- ABI 和 Bridge 在装载前协商版本；
- 所有公开接口都有最小运行示例；
- 实际基准结果已保存；
- Tessivum 产品固定一个 release/tag 后集成成功。

---

# 9. 公开契约草案

以下是设计约束，不是提前冻结的 Rust 语法。具体签名必须在对应里程碑用最小实现和测试确定。

## 9.1 标识

```text
ContextId
ScopeId
FiberId
PluginId
PluginInstanceId
ServiceKey
EventName
ResourceId
Generation
CancellationId
```

要求：

- 进程内标识不可与跨运行时 Handle 混用；
- 序列化格式稳定；
- generation 失效后拒绝旧 Handle；
- 日志可打印但不能泄露内存地址。

## 9.2 Fiber 状态

```text
Pending
Loading
Active
Failed
Unloading
Disposed
```

每次状态转换有单一提交点，并产生诊断事件。`Failed` 保留原始错误和 phase，但不能保留继续可调用的部分服务。

## 9.3 Plugin Runtime 管理面

```text
inspect(descriptor)
instantiate(context, config)
update(instance, config)
dispose(instance)
snapshot(instance)
```

Native、WASM、Legacy Node 可以有不同内部实现，但 Loader 只依赖这组生命周期语义。

## 9.4 跨运行时错误

```text
CordisError {
  code,
  message,
  phase,
  pluginId?,
  fiberId?,
  retryable,
  details?,
  sourceChain?
}
```

机器判断只使用稳定 code/phase。Node/WASM stack 是诊断信息，不参与控制流。

## 9.5 协议版本

至少独立版本化：

```text
cordis.plugin/v1       WASM ABI
cordis.node/v1         Node Bridge transport
service-name@1         每项跨运行时服务协议
```

Core ABI 版本不能替代领域服务版本。

# 10. Conformance 测试矩阵

## 10.1 Lifecycle

- sync/async start；
- sync/async cleanup；
- iterable cleanup；
- nested effect；
- reverse disposal；
- repeated/reentrant disposal；
- failed start rollback；
- parent/child quiescence；
- unload registration rejection。

## 10.2 Services

- late provider；
- provider replacement；
- provider removal/recovery；
- optional dependency；
- duplicate provider；
- isolate realms；
- shared realm；
- intercept merge；
- stale generation。

## 10.3 Events

- emit ordering；
- parallel aggregate；
- serial bail；
- sync bail；
- waterfall next/short-circuit/wrap；
- prepend；
- scoped filter；
- listener self-removal；
- owner disposal during dispatch。

## 10.4 Loader

- unordered dependencies；
- duplicate id；
- add/update/remove/move；
- config-only update；
- plugin replacement；
- candidate failure；
- rollback failure；
- nested group；
- patch insertion and later targeting；
- atomic write。

## 10.5 Cross-runtime

- Native ↔ WASM service/event；
- Native ↔ Node service/event；
- cancellation；
- timeout；
- oversize payload；
- stale Handle；
- Guest trap；
- Node crash/restart；
- protocol/ABI mismatch。

# 11. 性能基线

M8 前必须记录：

- 空 Context 内存；
- 每 1/100/1000 Fiber 的增量内存；
- Native plugin start/dispose；
- service lookup 和 replacement；
- emit/waterfall 吞吐；
- WASM cold/warm call；
- Node Bridge cold/warm/batched call；
- Loader 大树启动和事务更新；
- root dispose 到静默的延迟。

基准原则：

- 保存硬件、编译模式和样本；
- 分开冷启动和稳态；
- 报告中位数和高分位；
- 不用单次最佳结果；
- 性能优化不得破坏生命周期和错误语义。

# 12. 安全要求

## Native

完全可信，与 Host 同权限。动态 Native library 不进入第一版，除非有独立且经过审查的 ABI 需求。

## WASM

默认无能力。manifest 只声明请求，Host policy 决定实际授权。限制：

- memory；
- fuel/epoch/time；
- payload/output；
- 并发；
- WASI；
- HTTP hosts；
- capability methods；
- persistent variables。

## Legacy Node

可信旧代码。Bridge 提供故障隔离和资源归属，不提供安全沙箱。实际权限由 OS、容器或产品 Host policy 限制。

## 协议

- 所有输入先校验版本、长度和 schema；
- 未知消息类型 fail closed；
- 日志与 RPC frame 分离；
- secret 不进入默认诊断；
- 错误 details 有大小上限；
- Handle 不可由 Guest 自行构造有效值。

# 13. 版本与发布

## 13.1 Rust crate

阶段一使用 `0.x` 版本表达公开接口仍在收敛。当前仓库源码 release 为 `0.1.4`：四个
runtime crate 均为 `0.1.4`、使用 MIT License 且 `publish = false`，因此它是仓库源码
release，不是 crates.io 包发布。该 release 坐标独立于 `cordis.plugin/v1`、
`cordis.node/v1` 和 `tessivum.conformance/v1` conformance fixture schema。

每个 release/tag：
- 固定 changelog；
- 附 conformance 结果；
- 附 ABI/Bridge 版本；
- 附基准环境和结果；
- 由 Tessivum 产品兼容 CI 验证。

## 13.2 WASM ABI

- 主版本变化表示不兼容；
- 新增可选字段可以保持同主版本；
- 删除 Host Function 或改变语义必须升级主版本；
- Host 只维护明确列出的 ABI 版本窗口。

## 13.3 Node Bridge

Rust client 与 Node compat-host 必须在启动握手时协商。版本不兼容时在加载任何插件前退出，不能带着部分功能运行。

# 14. 工作方式

- `main` 始终可构建、可运行已完成里程碑的检查；
- 使用短期 feature branch，不建立长期 phase branch；
- 一个 PR 只跨越一个可验证契约；
- 修改公开行为必须先更新 fixture；
- 修改协议必须带兼容测试；
- 不在 Core 仓库加入 Tessivum 产品领域类型；
- 不把尚未使用的扩展点加入公开 API；
- 每个里程碑完成后才开始依赖它的下一个里程碑。

# 15. 第一项实施任务

第一项任务固定为 **M0：行为基线与差分夹具**，具体顺序：

1. 创建最小 Rust workspace 和 `fixtures/conformance`；
2. 定义 fixture 输入和标准化轨迹 schema；
3. 从 vendored Cordis 生命周期测试移植第一个“插件 start → effect → dispose”夹具；
4. 建立 TypeScript oracle runner；
5. 证明重复运行轨迹稳定；
6. 添加空 Rust runner 并让它以“未实现步骤”明确失败；
7. 再逐项加入 reentrant dispose、failed start、parent/child 和 service dependency。

在第一条跨语言稳定轨迹产生前，不接入 Extism、不实现 Loader、不创建产品 Agent 代码。
