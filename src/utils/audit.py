import os
import json
from datetime import datetime, timezone
from typing import Any, Dict

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def write_audit(audit_dir: str, name: str, payload: Dict[str, Any]) -> str:
    os.makedirs(audit_dir, exist_ok=True)
    out_path = os.path.join(audit_dir, f"{name}.json")

    payload = dict(payload)
    payload["written_at_utc"] = _utc_now_iso()

    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    return out_path