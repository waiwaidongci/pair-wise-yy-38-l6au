from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import uuid4

from .domain import (ConflictError, ValidationError, ensure_role,
                     normalize_severity, require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, BATCH_CATEGORY, BATCH_STATUSES, CREATE_ROLES,
                    ENTITY, RECORD_ROLES, STATES, TITLE, VIEW_ROLES,
                    completion_blockers, escalation_required, priority_score,
                    response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    @staticmethod
    def _resolve_request_id(request_id: Optional[str]) -> str:
        if request_id is None:
            return uuid4().hex
        if not isinstance(request_id, str):
            request_id = str(request_id)
        return require_text(request_id, "request_id", 100)

    @staticmethod
    def _replay_guard(batch: Dict[str, Any], action: str,
                      payload: Dict[str, Any]) -> None:
        if batch["action"] != action or batch["payload"] != payload:
            raise ConflictError("request_id已绑定不同的请求内容")

    def create_item(self, payload: Dict[str, Any], actor: str, role: str,
                    request_id: Optional[str] = None) -> Dict[str, Any]:
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
        request_id = self._resolve_request_id(request_id)
        canonical = {"title": title, "description": description, "severity": severity,
                     "quantity": quantity, "threshold": threshold,
                     "external_ref": external_ref, "actor": actor}
        detail = {"title": title, "severity": severity, "quantity": quantity,
                  "priority": priority_score(severity, quantity, threshold)}
        with self.repository.unit_of_work():
            existing = self.repository.find_batch(request_id)
            if existing is not None:
                self._replay_guard(existing, "create", canonical)
                if existing["status"] == "committed":
                    return self.enrich(existing["result"])
                raise ConflictError("请求尚未完成，请按原请求编号重试")
            item = self.repository.create_item(request_id, title, description, severity,
                                               quantity, threshold, external_ref, actor,
                                               canonical, detail)
            return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str, request_id: Optional[str] = None) -> Dict[str, Any]:
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
        request_id = self._resolve_request_id(request_id)
        canonical = {"kind": kind, "detail": detail, "status": status,
                     "external_ref": external_ref, "actor": actor}
        with self.repository.unit_of_work():
            self.repository.get_item(item_id)
            batch, is_new = self.repository.begin_batch(
                request_id, item_id, "record", actor, canonical)
            if not is_new:
                self._replay_guard(batch, "record", canonical)
                if batch["status"] == "committed":
                    return batch["result"]
                if batch["status"] in ("conflict", "failed"):
                    raise ConflictError(batch["error"] or "请求已被拒绝")
            return self.repository.complete_record_batch(
                batch["id"], item_id, kind, detail, status, external_ref, actor)

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str,
                   request_id: Optional[str] = None) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        if target not in STATES:
            raise ValidationError("未知状态")
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        ensure_role(role, role_for_transition(target))
        request_id = self._resolve_request_id(request_id)
        canonical = {"target": target, "expected_version": expected_version,
                     "actor": actor}
        with self.repository.unit_of_work():
            item = self.repository.get_item(item_id)
            batch, is_new = self.repository.begin_batch(
                request_id, item_id, "transition", actor, canonical, expected_version)
            if not is_new:
                self._replay_guard(batch, "transition", canonical)
                if batch["status"] == "committed":
                    return self.enrich(batch["result"])
                if batch["status"] in ("conflict", "failed"):
                    raise ConflictError(batch["error"] or "请求已被拒绝")
            # 批次和调度指令共用同一版本，版本不符一律按冲突处理
            if item["version"] != expected_version:
                self.repository.conflict_batch(batch["id"], "版本冲突，请刷新后重试")
                raise ConflictError("版本冲突，请刷新后重试")
            try:
                validate_transition(item["status"], target)
                blockers = completion_blockers(
                    target, self.repository.open_record_count(item_id))
                if blockers:
                    raise ConflictError("；".join(blockers))
            except (ConflictError, ValidationError) as exc:
                self.repository.fail_batch(batch["id"], str(exc))
                raise
            detail = {"from": item["status"], "to": target,
                      "escalation_required": escalation_required(
                          item["severity"], item["quantity"], item["threshold"])}
            updated = self.repository.complete_transition_batch(
                batch["id"], item_id, target, expected_version, actor, detail)
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

    def list_batches(self, role: str, item_id: Optional[int] = None,
                     status: Optional[str] = None) -> Dict[str, Any]:
        self._view(role)
        if status is not None and status not in BATCH_STATUSES:
            raise ValidationError("未知批次状态")
        batches = self.repository.list_batches(item_id, status)
        for batch in batches:
            batch["category"] = BATCH_CATEGORY[batch["status"]]
            batch["recovery_required"] = batch["status"] == "pending"
        summary = {"total": len(batches)}
        for name in BATCH_STATUSES:
            summary[name] = sum(1 for batch in batches if batch["status"] == name)
        summary["recovery_required"] = summary["pending"]
        return {"batches": batches, "summary": summary}

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

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
