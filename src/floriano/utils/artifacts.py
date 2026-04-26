from __future__ import annotations

import json
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    file_path = Path(path)
    digest = hashlib.sha256()

    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            digest.update(chunk)

    return digest.hexdigest()


@dataclass(frozen=True)
class LatestArtifactPointer:
    artifact_name: str
    run_id: str
    path: str
    artifact_hash: str
    record_count: int
    status: str
    module: str
    manifest_path: Optional[str]
    published_at_utc: str


class LatestArtifactRegistry:
    """
    Registry operacional dos artefatos latest.

    Regra:
    - cada artefato latest aponta para exatamente um run_id;
    - latest só deve apontar para artefato aprovado;
    - data/raw/_latest não deve ser commitado.
    """

    def __init__(self, base_dir: str | Path = "data/raw/_latest") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def pointer_path(self, artifact_name: str) -> Path:
        safe_name = artifact_name.strip().replace("/", "_")
        return self.base_dir / f"{safe_name}.json"

    def publish(
        self,
        *,
        artifact_name: str,
        run_id: str,
        path: str | Path,
        record_count: int,
        status: str,
        module: str,
        manifest_path: Optional[str] = None,
        artifact_hash: Optional[str] = None,
    ) -> Path:
        artifact_path = Path(path)

        if not artifact_path.exists():
            raise FileNotFoundError(f"Artefato não encontrado: {artifact_path}")

        if status != "SUCCESS":
            raise ValueError("LatestArtifactRegistry só publica status SUCCESS.")

        resolved_hash = artifact_hash or sha256_file(artifact_path)

        pointer = LatestArtifactPointer(
            artifact_name=artifact_name,
            run_id=run_id,
            path=str(artifact_path),
            artifact_hash=resolved_hash,
            record_count=record_count,
            status=status,
            module=module,
            manifest_path=manifest_path,
            published_at_utc=utc_now(),
        )

        output_path = self.pointer_path(artifact_name)
        output_path.write_text(
            json.dumps(asdict(pointer), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return output_path

    def read(self, artifact_name: str) -> Dict[str, Any]:
        path = self.pointer_path(artifact_name)

        if not path.exists():
            raise FileNotFoundError(f"Latest pointer não encontrado: {path}")

        return json.loads(path.read_text(encoding="utf-8"))


class ArtifactManifestManager:
    """
    Manifesto de linhagem de artefatos.

    Registra:
    - inputs;
    - outputs;
    - hashes;
    - contagens;
    - status;
    - módulo executor.
    """

    def __init__(self, base_dir: str | Path = "data/raw/_manifests") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write_manifest(
        self,
        *,
        run_id: str,
        module: str,
        status: str,
        input_artifacts: List[Dict[str, Any]],
        output_artifacts: List[Dict[str, Any]],
        record_counts: Dict[str, int],
        extra: Optional[Dict[str, Any]] = None,
    ) -> Path:
        run_dir = self.base_dir / f"run_id={run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

        manifest = {
            "run_id": run_id,
            "module": module,
            "status": status,
            "input_artifacts": input_artifacts,
            "output_artifacts": output_artifacts,
            "record_counts": record_counts,
            "extra": extra or {},
            "created_at_utc": utc_now(),
        }

        path = run_dir / "artifact_manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
