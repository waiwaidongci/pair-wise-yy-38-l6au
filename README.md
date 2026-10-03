# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8315
```

默认端口为`8315`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit`

### 处置批次（现场反馈 + 总工授权）

把「值班员现场反馈」与「总工授权」两步收进同一处置批次，避免只留半条处理痕迹：

- `POST /api/items/{id}/batches`：值班员提交现场反馈（批次前半程），需带 `request_no`。反馈记录、状态复核（draft→checked）、审计与批次在同一事务内写入；同一 `request_no` 重复提交幂等返回。
- `POST /api/batches/{request_no}/authorize`：总工授权（批次后半程），需带 `expected_version`。批次与调度指令共用同一版本，按版本做 compare-and-swap，先到者生效；冲突时批次标记为 `conflict` 且不写授权审计。
- `GET /api/batches`：批次列表，可按 `?status=`、`?item_id=` 过滤。每项含 `progress`（`feedback_done`/`committed`/`conflict`）与 `recoverable`，用于区分批次进度、冲突与待恢复项。
- `GET /api/batches/{request_no}`：单个批次详情。

后半程失败后，按同一 `request_no` 并携带新的 `expected_version` 重新授权即可恢复，审计不重复追加。首次升级旧库时，会为历史审计按原顺序补 `batch_id` 归属（`schema_migrations` 保证只执行一次），`batch_id` 不参与哈希载荷，哈希链保持不变。

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
