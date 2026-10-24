#!/usr/bin/env python3
"""Apply annotation-hub's integration patches to its local Multiomics checkouts."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


class PatchError(RuntimeError):
    """A checkout cannot safely receive the expected local patch."""


_MULTIOMICS_IDENTIFIERS = frozenset(
    {
        "multiomics_clinicaltrials_kp",
        "multiomics_drug_approvals_kp",
        "clinical_trials_kp",
        "drug_approvals_kp",
        "multiomics_clinical_trials",
        "multiomics_drug_approvals",
        "multiomics_clinical_trials_annotator",
        "multiomics_drug_approvals_annotator",
    }
)


def block_unprepared_multiomics(manager, logger) -> None:
    """Reject Multiomics registration for this manager, including later reloads."""
    if getattr(manager, "_multiomics_registration_blocked", False):
        return
    register_classes = manager.register_classes

    def register_prepared_classes(klasses):
        allowed = []
        for klass in klasses:
            names = (
                getattr(klass, key, None) for key in ("SRC_NAME", "name", "main_source")
            )
            blocked = any(
                isinstance(name, str) and name in _MULTIOMICS_IDENTIFIERS
                for name in names
            )
            for key in ("DATA_PLUGIN_FOLDER", "SRC_ROOT_FOLDER"):
                folder = getattr(klass, key, None)
                if isinstance(folder, (str, os.PathLike)):
                    blocked = blocked or Path(folder).name in _MULTIOMICS_IDENTIFIERS
            if blocked:
                logger.error(
                    "Skipping unprepared Multiomics source class %s", klass.__name__
                )
            else:
                allowed.append(klass)
        if allowed:
            return register_classes(allowed)
        return None

    manager.register_classes = register_prepared_classes
    manager._multiomics_registration_blocked = True


def _git(folder: Path, *arguments: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(folder), *arguments], capture_output=True, check=False
    )
    if result.returncode:
        raise PatchError(result.stderr.decode().strip())
    return result.stdout


def _known_manifest(content: bytes, upstream: bytes, patched: bytes) -> bool:
    """Also accept su06's existing annotation-only manifest override."""
    try:
        current = json.loads(content)
        original = json.loads(upstream)
        annotation_only = json.loads(upstream)
        annotation_only["uploaders"] = [
            uploader
            for uploader in original["uploaders"]
            if uploader["parser"] == "annotator_parser:load_data"
        ]
        return current in (original, annotation_only, json.loads(patched))
    except (ValueError, KeyError, TypeError):
        return False


def _plugin_plan(root: Path, entry: dict, patch_folder: Path) -> list:
    plugin = root / entry["path"]
    if plugin.is_symlink():
        raise PatchError(f"Refusing to patch symlinked plugin {plugin}")
    toplevel = Path(_git(plugin, "rev-parse", "--show-toplevel").decode().strip())
    if toplevel.resolve() != plugin.resolve():
        raise PatchError(f"{entry['path']} is not an initialized submodule")
    head = _git(plugin, "rev-parse", "HEAD").decode().strip()
    if head != entry["commit"]:
        raise PatchError(
            f"{entry['path']} is at {head}, expected {entry['commit']}; "
            "review the local patches before updating their pins"
        )

    originals = {
        name: _git(plugin, "show", f"{head}:{name}") for name in entry["files"]
    }
    with tempfile.TemporaryDirectory(prefix="annotation-hub-multiomics-") as tmp:
        staging = Path(tmp)
        for name, content in originals.items():
            (staging / name).write_bytes(content)
        _git(staging, "apply", str(patch_folder / entry["patch"]))
        patched = {name: (staging / name).read_bytes() for name in originals}

    plan = []
    for name, upstream in originals.items():
        target = plugin / name
        if target.is_symlink():
            raise PatchError(f"Refusing to replace symlink {target}")
        current = target.read_bytes()
        if current == patched[name]:
            continue
        known = current == upstream
        if name == "manifest.json":
            known = _known_manifest(current, upstream, patched[name])
        if not known:
            raise PatchError(
                f"Unrecognized local edits in {target}; leaving all files unchanged"
            )
        plan.append((target, patched[name]))
    return plan


def prepare_multiomics(
    root: str | Path, *, check: bool = False, allow_missing: bool = False
) -> list[str]:
    """Preflight both plugins, then apply only recognized local file changes.

    Neither submodule commits nor remotes are changed. Unknown local edits or a
    different upstream revision stop preparation before any file is written.
    """
    root = Path(root).resolve()
    patch_folder = root / "patches" / "multiomics"
    entries = json.loads((patch_folder / "patches.json").read_text())
    plan = []
    for entry in entries:
        plugin = root / entry["path"]
        if not (plugin / ".git").exists():
            if allow_missing:
                continue
            raise PatchError(
                f"Initialize {entry['path']} with git submodule update --init first"
            )
        plan.extend(_plugin_plan(root, entry, patch_folder))

    if not check:
        for target, content in plan:
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as tmp:
                temporary = Path(tmp.name)
                try:
                    tmp.write(content)
                    tmp.flush()
                    os.chmod(temporary, target.stat().st_mode)
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
    return [str(target.relative_to(root)) for target, _ in plan]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="report needed patches without writing"
    )
    arguments = parser.parse_args()
    try:
        changed = prepare_multiomics(
            Path(__file__).resolve().parents[1], check=arguments.check
        )
    except (PatchError, OSError) as error:
        parser.exit(2, f"Multiomics preparation failed: {error}\n")
    for name in changed:
        print(f"{'Needs patch' if arguments.check else 'Patched'}: {name}")
    if not changed:
        print("Multiomics local patches are already applied.")
    return int(arguments.check and bool(changed))


if __name__ == "__main__":
    raise SystemExit(main())
