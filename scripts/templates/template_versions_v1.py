from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
MANIFEST_VERSION_V1 = "saved_report_template_manifest_v1"
VersionPolicy = Literal["new_revision", "replace_active"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def template_package_dir(templates_dir: Path, template_id: str) -> Path:
    return templates_dir / template_id


def legacy_flat_path(templates_dir: Path, template_id: str) -> Path:
    return templates_dir / f"{template_id}.json"


def manifest_path(templates_dir: Path, template_id: str) -> Path:
    return template_package_dir(templates_dir, template_id) / "manifest.json"


def revision_doc_path(templates_dir: Path, template_id: str, revision: int) -> Path:
    return template_package_dir(templates_dir, template_id) / f"v{revision}.json"


def _validate_template_id(template_id: str) -> str:
    tid = str(template_id or "").strip()
    if not tid or not _ID_RE.match(tid):
        raise ValueError("template_id must match [A-Za-z0-9_-]+")
    return tid


def read_manifest(templates_dir: Path, template_id: str) -> Optional[Dict[str, Any]]:
    path = manifest_path(templates_dir, template_id)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("manifest must be a JSON object")
    if data.get("manifest_version") != MANIFEST_VERSION_V1:
        raise ValueError(f"unsupported manifest_version: {data.get('manifest_version')!r}")
    if str(data.get("template_id") or "").strip() != template_id:
        raise ValueError("manifest template_id does not match directory")
    return data


def write_manifest(templates_dir: Path, manifest: Dict[str, Any]) -> None:
    tid = _validate_template_id(str(manifest.get("template_id") or ""))
    pkg = template_package_dir(templates_dir, tid)
    pkg.mkdir(parents=True, exist_ok=True)
    manifest_path(templates_dir, tid).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def list_revision_numbers(manifest: Dict[str, Any]) -> List[int]:
    revs = manifest.get("revisions")
    if not isinstance(revs, list):
        return []
    out: List[int] = []
    for row in revs:
        if isinstance(row, dict) and isinstance(row.get("revision"), int):
            out.append(int(row["revision"]))
    return sorted(set(out))


def has_versioned_package(templates_dir: Path, template_id: str) -> bool:
    return manifest_path(templates_dir, template_id).exists()


def has_legacy_flat(templates_dir: Path, template_id: str) -> bool:
    return legacy_flat_path(templates_dir, template_id).exists()


def is_published_on_disk(templates_dir: Path, template_id: str) -> bool:
    return has_versioned_package(templates_dir, template_id) or has_legacy_flat(
        templates_dir, template_id
    )


def load_revision_doc(
    templates_dir: Path, template_id: str, revision: int
) -> Dict[str, Any]:
    path = revision_doc_path(templates_dir, template_id, revision)
    if not path.exists():
        raise FileNotFoundError(f"Missing revision file: {path}")
    doc = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError(f"revision doc must be an object: {path}")
    return doc


def load_active_template_doc(templates_dir: Path, template_id: str) -> Optional[Dict[str, Any]]:
    tid = _validate_template_id(template_id)
    manifest = read_manifest(templates_dir, tid)
    if manifest is not None:
        active = int(manifest.get("active_revision") or 0)
        if active < 1:
            raise ValueError(f"invalid active_revision for {tid}")
        return load_revision_doc(templates_dir, tid, active)
    legacy = legacy_flat_path(templates_dir, tid)
    if legacy.exists():
        doc = json.loads(legacy.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else None
    return None


def list_template_versions(templates_dir: Path, template_id: str) -> Dict[str, Any]:
    tid = _validate_template_id(template_id)
    manifest = read_manifest(templates_dir, tid)
    if manifest is None:
        if has_legacy_flat(templates_dir, tid):
            return {
                "template_id": tid,
                "versioning": "legacy_flat",
                "active_revision": 1,
                "revisions": [
                    {
                        "revision": 1,
                        "published_at": None,
                        "is_active": True,
                        "path": str(legacy_flat_path(templates_dir, tid)),
                    }
                ],
            }
        raise FileNotFoundError(f"No published template package for {tid}")

    active = int(manifest.get("active_revision") or 0)
    revisions_out: List[Dict[str, Any]] = []
    for row in manifest.get("revisions") or []:
        if not isinstance(row, dict):
            continue
        rev = row.get("revision")
        if not isinstance(rev, int):
            continue
        revisions_out.append(
            {
                "revision": rev,
                "published_at": row.get("published_at"),
                "is_active": rev == active,
                "path": str(revision_doc_path(templates_dir, tid, rev)),
            }
        )
    revisions_out.sort(key=lambda r: int(r["revision"]))
    return {
        "template_id": tid,
        "versioning": "revision_package",
        "manifest_version": MANIFEST_VERSION_V1,
        "active_revision": active,
        "revisions": revisions_out,
    }


def _new_manifest(template_id: str) -> Dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION_V1,
        "template_id": template_id,
        "active_revision": 0,
        "revisions": [],
    }


def _write_revision_doc(path: Path, doc: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _migrate_legacy_to_package(
    templates_dir: Path, template_id: str, *, published_at: Optional[str] = None
) -> Dict[str, Any]:
    legacy = legacy_flat_path(templates_dir, template_id)
    if not legacy.exists():
        raise FileNotFoundError(f"No legacy flat template to migrate: {legacy}")
    doc = json.loads(legacy.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError("legacy template file must contain a JSON object")
    manifest = _new_manifest(template_id)
    manifest["active_revision"] = 1
    manifest["revisions"] = [
        {
            "revision": 1,
            "published_at": published_at or _utc_now_iso(),
            "migrated_from_legacy": True,
        }
    ]
    _write_revision_doc(revision_doc_path(templates_dir, template_id, 1), doc)
    write_manifest(templates_dir, manifest)
    legacy.unlink()
    return manifest


def publish_template_versioned(
    templates_dir: Path,
    tpl_doc: Dict[str, Any],
    *,
    version_policy: VersionPolicy = "new_revision",
) -> Dict[str, Any]:
    tid = _validate_template_id(str(tpl_doc.get("template_id") or ""))
    templates_dir.mkdir(parents=True, exist_ok=True)
    migrated_from_legacy = False
    manifest = read_manifest(templates_dir, tid)

    if manifest is None and has_legacy_flat(templates_dir, tid):
        manifest = _migrate_legacy_to_package(templates_dir, tid)
        migrated_from_legacy = True

    if manifest is None:
        manifest = _new_manifest(tid)

    existing_revs = list_revision_numbers(manifest)
    active = int(manifest.get("active_revision") or 0)

    if version_policy == "replace_active":
        if not existing_revs and not migrated_from_legacy:
            target_rev = 1
            manifest["revisions"] = [{"revision": 1, "published_at": _utc_now_iso()}]
        else:
            if active < 1:
                active = max(existing_revs) if existing_revs else 1
            target_rev = active
            for row in manifest.get("revisions") or []:
                if isinstance(row, dict) and row.get("revision") == target_rev:
                    row["published_at"] = _utc_now_iso()
                    break
        manifest["active_revision"] = target_rev
        write_manifest(templates_dir, manifest)
        out_path = revision_doc_path(templates_dir, tid, target_rev)
        _write_revision_doc(out_path, tpl_doc)
        return {
            "template_id": tid,
            "revision": target_rev,
            "active_revision": target_rev,
            "version_policy": version_policy,
            "path": str(out_path),
            "revisions_count": len(list_revision_numbers(manifest)),
            "migrated_from_legacy": migrated_from_legacy,
            "created_new_revision": False,
        }

    next_rev = (max(existing_revs) + 1) if existing_revs else 1
    manifest["active_revision"] = next_rev
    rev_rows = [r for r in (manifest.get("revisions") or []) if isinstance(r, dict)]
    rev_rows.append({"revision": next_rev, "published_at": _utc_now_iso()})
    manifest["revisions"] = sorted(rev_rows, key=lambda r: int(r["revision"]))
    write_manifest(templates_dir, manifest)
    out_path = revision_doc_path(templates_dir, tid, next_rev)
    _write_revision_doc(out_path, tpl_doc)
    return {
        "template_id": tid,
        "revision": next_rev,
        "active_revision": next_rev,
        "version_policy": version_policy,
        "path": str(out_path),
        "revisions_count": len(list_revision_numbers(manifest)),
        "migrated_from_legacy": migrated_from_legacy,
        "created_new_revision": next_rev > 1 or migrated_from_legacy,
    }


def activate_template_revision(
    templates_dir: Path, template_id: str, revision: int
) -> Dict[str, Any]:
    """
    Point the published package at an existing revision (rollback / promote) without
    creating a new revision file.
    """
    tid = _validate_template_id(template_id)
    if revision < 1:
        raise ValueError("revision must be >= 1")

    manifest = read_manifest(templates_dir, tid)
    if manifest is None:
        if has_legacy_flat(templates_dir, tid):
            if revision != 1:
                raise ValueError("legacy flat template only has revision 1")
            doc = load_active_template_doc(templates_dir, tid)
            return {
                "template_id": tid,
                "active_revision": 1,
                "revision": 1,
                "path": str(legacy_flat_path(templates_dir, tid)),
                "versioning": "legacy_flat",
            }
        raise FileNotFoundError(f"No published template package for {tid}")

    revs = list_revision_numbers(manifest)
    if revision not in revs:
        raise ValueError(
            f"revision v{revision} does not exist for {tid} (have: {revs})"
        )
    if not revision_doc_path(templates_dir, tid, revision).exists():
        raise FileNotFoundError(f"Missing revision file v{revision} for {tid}")

    previous = int(manifest.get("active_revision") or 0)
    manifest["active_revision"] = revision
    write_manifest(templates_dir, manifest)
    return {
        "template_id": tid,
        "active_revision": revision,
        "revision": revision,
        "previous_active_revision": previous if previous > 0 else None,
        "path": str(revision_doc_path(templates_dir, tid, revision)),
        "versioning": "revision_package",
    }


def unpublish_template(templates_dir: Path, template_id: str) -> Dict[str, Any]:
    tid = _validate_template_id(template_id)
    pkg = template_package_dir(templates_dir, tid)
    if pkg.exists() and pkg.is_dir() and has_versioned_package(templates_dir, tid):
        shutil.rmtree(pkg)
        return {"template_id": tid, "removed": "revision_package", "path": str(pkg)}
    legacy = legacy_flat_path(templates_dir, tid)
    if legacy.exists():
        legacy.unlink()
        return {"template_id": tid, "removed": "legacy_flat", "path": str(legacy)}
    raise FileNotFoundError(f"No published template found for {tid}")


def load_all_active_template_docs(templates_dir: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not templates_dir.exists():
        return out

    for child in sorted(templates_dir.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "manifest.json").exists():
            continue
        tid = child.name
        try:
            doc = load_active_template_doc(templates_dir, tid)
        except Exception:
            continue
        if doc is not None:
            out[tid] = doc

    for p in sorted(templates_dir.glob("*.json")):
        tid = p.stem
        if tid in out or has_versioned_package(templates_dir, tid):
            continue
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(doc, dict):
            out[tid] = doc
    return out
