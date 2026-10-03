# 水库防汛调度与操作确认

根据库位、入库流量、下游警戒和施工限制生成复核授权的泄洪指令。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限、批次状态和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制、处置批次、迁移和审计链。
- `src/service.py`：权限检查、用例编排、并发控制、幂等恢复和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则、失败和批次测试。

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
- `GET /api/batches`，可按`item_id`、`status`过滤

允许角色：duty_officer, chief_engineer, dispatcher, viewer。库位超过汛限或入库流量上升时提升紧迫度；授权前必须有复核记录，执行后仍要闭环现场反馈。

## 处置批次与幂等

值班员改现场反馈、总工授权等写操作都收进处置批次：批次和调度指令共用同一版本，状态变更与审计记录在同一事务里写入，要么一起落库要么都不留痕；并发提交时先到者生效，后到者批次记为冲突。

- 客户端用`request_id`请求体字段或`X-Request-Id`请求头携带请求编号；同一编号重放返回首个结果，不会重复追加审计。编号若绑定了不同请求内容则按冲突拒绝。
- 后半程失败后按同一请求编号重新提交即可恢复：已提交的直接返回原结果，中断留下的待恢复批次会继续执行，审计仍只追加一次。
- 批次状态：`pending`（待恢复）、`committed`、`conflict`、`failed`；`GET /api/batches`按`category`区分批次进度（progress）、冲突（conflict）和待恢复项（recovery），并给出汇总计数。
- 旧数据第一次升级时自动迁移：已有审计记录按原顺序补上批次归属（`legacy-audit-{id}`），只写归属字段，哈希链保持不变；迁移只执行一次。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
