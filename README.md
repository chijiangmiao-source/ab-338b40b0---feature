# 近地轨道辐射监测站（LEO Radiation Monitor）

接收两台独立探头（`alpha` / `beta`）乱序、可能重传的剂量读数，按**五分钟窗口**
封存总剂量、峰值与告警等级。已封存的窗口**不可回写**：迟到包无法改写已发布的
风险等级，进程重启后封存记录与幂等状态完整保留。

## 核心语义

- **幂等重传**：`event_id` 是稳定事件标识。同内容重传原样回放首次提交的应答
  （HTTP 200，响应头 `X-Idempotent-Replay: true`，响应体逐字节一致）；同标识
  不同内容返回 `409 event_conflict`。同一探头同一 `seq` 绑定不同事件标识返回
  `409 seq_conflict`。
- **水位推进**：每个探头只统计**连续序号前缀**（从 1 开始、无缺口）的最大观测
  时刻作为进度；水位 = `min(两探头进度) − ALLOWED_LATENESS_SECONDS`。序号跳跃、
  倒退或探头尚未出现都不会伪造水位（水位为空或不前进）。水位单调不回退。
- **事务化封存**：只有结束时刻 ≤ 水位的窗口才能封存，且封存与触发它的读数在
  **同一个 SQLite 事务**（`BEGIN IMMEDIATE`）中提交；`window_start_ms` 为主键，
  并发提交与进程重启都只能留下唯一封存记录。封存前乱序到达的数据被准确并入；
  封存后落入该窗口的数据返回 `409 window_sealed` 且不改变任何汇总。结束时刻
  不晚于当前水位的窗口（包括尚无物理记录的空窗口）一律拒绝迟到数据。
- **可复核性**：封存记录包含首个违规事件（按观测时刻排序，首个使窗口脱离
  NORMAL 的事件：自身剂量越限，或累计剂量在该事件处越限）以及封存瞬间两侧
  探头的进度（连续序号、进度时刻）与水位，可通过读取接口复查。
- **连续辐照事件**：值班员查看已封存结果时，相邻且非 NORMAL 的窗口汇成一次
  连续辐照事件，避免把同一次持续告警人工拆成多条。引擎在封存窗口的**同一事务**
  内以窗口起点连续性推进**唯一**打开事件：ELEVATED/CRITICAL 窗口创建或扩展它，
  下一个已封存的 NORMAL 窗口才将其结束（水位跨过的空窗口也按 NORMAL 参与截断）。
  事件含起止窗口、覆盖窗口数、累计剂量、期间峰值与最高风险等级，并区分
  **ongoing（仍在延续）**与 **ended（已结束）**。事件一旦结束即不可变，迟到读数、
  重放或窗口读取都不会改写它。

告警等级（阈值均可配置）：

| 等级 | 条件 |
| --- | --- |
| `CRITICAL` | 总剂量 ≥ `CRITICAL_TOTAL` 或峰值 ≥ `CRITICAL_PEAK` |
| `ELEVATED` | 总剂量 ≥ `ELEVATED_TOTAL` 或峰值 ≥ `ELEVATED_PEAK` |
| `NORMAL` | 其余 |

## HTTP 接口

### `POST /readings` — 提交读数

```json
{
  "event_id": "alpha-0001",
  "probe": "alpha",
  "seq": 1,
  "observed_at": "2026-10-05T00:00:10Z",
  "dose": 12.5
}
```

- `200`：应答含所属窗口、当前水位、本次提交封存的窗口列表、两侧探头进度；
  同内容重传回放同一应答。
- `400 unknown_probe`：探头未配置；`409 event_conflict / seq_conflict /
  window_sealed`：内容冲突、序号复用、落入已封存（或已过水位）窗口。
- `observed_at` 必须带时区，否则 `422`。

### `GET /windows/{window_start}` — 按窗口读取结果

`window_start` 为 ISO 8601 时刻（自动对齐到所属窗口起点）。已封存窗口返回
不可变的发布记录（`status: "sealed"`，含总剂量、峰值、等级、首个违规事件、
`progress_at_seal`、`watermark_at_seal`、`sealed_at` 与窗口内事件清单）；未封存
窗口返回明确标记的临时视图（`status: "open"`，`accepting` 表示是否仍接收数据）。

### 其他

- `GET /windows?since=&until=`：列出已封存窗口。
- `GET /incidents?since=&until=&status=`：按事件起点稳定排序列出连续辐照事件，
  每条含起止窗口、`status`（`ongoing`/`ended`）、覆盖窗口数、累计剂量、期间峰值、
  `highest_level`、`closed_at` 与构成窗口清单。
- `GET /incidents/{window_start}`：返回覆盖该窗口（自动对齐五分钟边界）的事件；
  NORMAL 或未封存窗口不属于任何事件，返回 `404 no_incident`。
- `GET /state`：水位、两探头进度、计数器（含进行中/已结束事件数）与生效配置。
- `GET /health`：健康检查（含数据库探活），Compose 健康检查使用。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `HOST_PORT` | `8000` | **宿主机**映射端口（仅 Compose） |
| `PORT` | `8000` | 容器内监听端口 |
| `DATABASE_PATH` | `/data/radiation.db`（Compose） | SQLite 数据文件 |
| `WINDOW_SECONDS` | `300` | 窗口长度（五分钟） |
| `ALLOWED_LATENESS_SECONDS` | `60`（Compose 为 `30`） | 允许迟到量 |
| `ELEVATED_TOTAL` / `ELEVATED_PEAK` | `100` / `40` | ELEVATED 阈值 |
| `CRITICAL_TOTAL` / `CRITICAL_PEAK` | `250` / `80` | CRITICAL 阈值 |
| `PROBE_ALPHA` / `PROBE_BETA` | `alpha` / `beta` | 探头标识 |

## 运行

```bash
docker compose up --build app          # 默认宿主机端口 8000
HOST_PORT=9000 docker compose up app   # 自定义宿主机端口
curl localhost:9000/health
```

数据保存在命名卷 `rad-data` 中，容器重建、进程重启后状态完整恢复。

## 验收（一次性 verify 容器）

```bash
docker compose up --build --exit-code-from verify
echo $?   # 0 = 全部通过
```

`verify` 等待 `app` 健康后依次执行（任一失败即以非零退出码结束）：

1. **构建检查**：字节编译全部源码并导入应用模块；
2. **代码测试**：`pytest`（重传幂等/冲突、乱序并入与封存、水位诚实性、
   阈值与首个违规事件、连续辐照事件归并与不可变性、重启恢复、并发唯一封存等
   30 个用例）；
3. **HTTP 冒烟**：对运行中的服务验证重传回放与冲突、乱序封存与迟到拒绝，
   并通过 Docker socket **重启 app 容器**验证封存记录、幂等回放与水位
   在重启后保持不变。

重复验收前建议 `docker compose down -v` 清理数据卷（冒烟脚本按 `/state`
中的探头进度续接序号，也可直接在既有数据上重跑）。

## 本地开发

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
pytest tests -q
uvicorn app.main:app --port 8000
```

## 结构

```
app/
  config.py    # 环境变量配置
  models.py    # 请求模型（event_id/probe/seq/observed_at/dose）
  storage.py   # SQLite（WAL）：读数、探头进度、封存窗口、连续辐照事件；单连接+锁串行化
  engine.py    # 幂等摄入、连续序号进度、水位、同事务封存与事件归并、等级与违规判定
  main.py      # FastAPI：POST /readings、GET /windows…、/incidents…、/state、/health
tests/         # pytest 验收用例
scripts/
  smoke.py     # HTTP 冒烟（重传、乱序封存、重启恢复）
  verify.sh    # verify 容器入口：构建检查 + 测试 + 冒烟
Dockerfile     # runtime / verify 两个构建目标（Python 3.13）
docker-compose.yml
```
