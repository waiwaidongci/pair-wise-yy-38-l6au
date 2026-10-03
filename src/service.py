from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ValidationError, ensure_role, normalize_severity,
                     require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, RECORD_ROLES, TITLE,
                    VIEW_ROLES, completion_blockers, escalation_required,
                    priority_score, response_deadline_hours, role_for_transition,
                    validate_transition)

# 处置批次角色：值班员现场反馈，总工授权。
BATCH_FEEDBACK_ROLES = set(['duty_officer'])
BATCH_AUTHORIZE_ROLES = set(['chief_engineer'])
BATCH_PROGRESS = {
    'pending': 'feedback_done',   # 已反馈，待授权（可恢复）
    'committed': 'committed',     # 两步均完成
    'conflict': 'conflict',       # 授权冲突，待恢复
}


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            from .domain import ConflictError
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    # ------------------------------------------------------------------ #
    # 处置批次：现场反馈 + 总工授权
    # ------------------------------------------------------------------ #

    def submit_feedback(self, item_id: int, payload: Dict[str, Any],
                        actor: str, role: str) -> Dict[str, Any]:
        """值班员现场反馈（批次前半程）：状态与审计同生共死。"""
        ensure_role(role, BATCH_FEEDBACK_ROLES)
        actor = require_text(actor, "actor", 100)
        request_no = require_text(payload.get("request_no"), "request_no", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValidationError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        batch = self.repository.create_batch_feedback(
            item_id, request_no, kind, detail, status, external_ref, actor)
        return self.enrich_batch(batch)

    def authorize(self, request_no: str, payload: Dict[str, Any],
                  actor: str, role: str) -> Dict[str, Any]:
        """总工授权（批次后半程）：先到者生效，失败按原请求编号恢复，不重复审计。"""
        ensure_role(role, BATCH_AUTHORIZE_ROLES)
        actor = require_text(actor, "actor", 100)
        expected_version = payload.get("expected_version")
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValidationError("expected_version必须是正整数")
        batch = self.repository.authorize_batch(request_no, expected_version, actor)
        return self.enrich_batch(batch)

    def list_batches(self, role: str, status: Optional[str] = None,
                     item_id: Optional[int] = None) -> list:
        self._view(role)
        if status is not None and status not in BATCH_PROGRESS:
            raise ValidationError("批次状态必须是pending、committed或conflict")
        return [self.enrich_batch(b)
                for b in self.repository.list_batches(status, item_id)]

    def get_batch(self, request_no: str, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich_batch(self.repository.get_batch(request_no))

    @staticmethod
    def enrich_batch(batch: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(batch)
        status = batch.get("status")
        result["progress"] = BATCH_PROGRESS.get(status, status)
        result["recoverable"] = status in ("pending", "conflict")
        return result

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
