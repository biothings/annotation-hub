"""
Guards the `import biothings.hub` bootstrap in the data plugin packages.

`biothings/__init__.py` does not define `config`. It is installed as a side
effect of importing `biothings.hub`, which also fills in the
`biothings.utils.hub_db` placeholders (`get_src_db()` raises
NotImplementedError until then).

The hub process does that at startup, but the uploaders run their work in a
`ProcessPoolExecutor` whose children are fresh interpreters (nodenorm asks for
"spawn" explicitly; Python 3.14 defaults to "forkserver" on Linux). Those
children import the plugin package while unpickling their task, so the package
has to bootstrap the config before any module runs `from biothings import
config` -- otherwise every child dies with

    ImportError: cannot import name 'config' from 'biothings'

and the parent only ever sees `BrokenProcessPool`, which hides the cause.

The bootstrap import looks unused, so these tests assert it is present and
ordered ahead of the plugin imports.
"""

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).parents[1]
PLUGIN_ROOT = REPOSITORY_ROOT / "plugins"
BOOTSTRAP_MODULE = "biothings.hub"


def _module_level_imports(source: str) -> list[str]:
    """Return the imported module names, in order, for module-level imports."""
    imported = []
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            # level > 0 is a relative import, i.e. a sibling plugin module
            imported.append("." * node.level + (node.module or ""))
    return imported


@pytest.mark.parametrize(
    "plugin_name",
    [
        pytest.param("nameres", id="nameres"),
        pytest.param("nodenorm", id="nodenorm"),
    ],
)
def test_plugin_package_bootstraps_biothings_config(plugin_name):
    package_init = PLUGIN_ROOT / plugin_name / "__init__.py"
    imported = _module_level_imports(package_init.read_text(encoding="utf-8"))

    assert BOOTSTRAP_MODULE in imported, (
        f"{package_init} must import {BOOTSTRAP_MODULE} so worker processes "
        "started with 'spawn'/'forkserver' can resolve 'biothings.config'"
    )

    relative_imports = [name for name in imported if name.startswith(".")]
    assert relative_imports, f"expected {package_init} to import plugin modules"
    assert imported.index(BOOTSTRAP_MODULE) < imported.index(relative_imports[0]), (
        f"{package_init} must import {BOOTSTRAP_MODULE} before its own modules, "
        "which read 'biothings.config' at import time"
    )


@pytest.mark.parametrize(
    "plugin_name",
    [
        pytest.param("nameres", id="nameres"),
        pytest.param("nodenorm", id="nodenorm"),
    ],
)
def test_plugin_modules_still_read_config_at_import_time(plugin_name):
    """
    The bootstrap is only needed while the plugin modules resolve
    `biothings.config` during import. If that ever stops being true the
    assertions above can go too, so pin the assumption rather than letting it
    drift silently.
    """
    plugin_modules = sorted(
        path
        for path in (PLUGIN_ROOT / plugin_name).glob("*.py")
        if path.name != "__init__.py"
    )
    assert plugin_modules, f"no plugin modules found for {plugin_name}"

    importers = [
        path.name
        for path in plugin_modules
        if "from biothings import config" in path.read_text(encoding="utf-8")
    ]
    assert importers, (
        f"no {plugin_name} module reads 'biothings.config' at import time any "
        "more; re-evaluate whether the bootstrap import is still required"
    )


def test_spawned_nodenorm_worker_bootstraps_config_and_hub_db(tmp_path):
    config_module_name = "spawn_test_hub_config"
    sqlite_folder = tmp_path / "hubdb"
    archive_folder = tmp_path / "archive"
    log_folder = tmp_path / "logs"
    (tmp_path / f"{config_module_name}.py").write_text(
        textwrap.dedent(f"""
            import logging

            HUB_DB_BACKEND = {{
                "module": "biothings.utils.sqlite3",
                "sqlite_db_folder": {str(sqlite_folder)!r},
            }}
            DATA_HUB_DB_DATABASE = "spawn_hub"
            DATA_SRC_DATABASE = "spawn_src"
            DATA_ARCHIVE_ROOT = {str(archive_folder)!r}
            LOG_FOLDER = {str(log_folder)!r}
            logger = logging.getLogger("spawn-test-hub")


            def source_db_name():
                from biothings.utils.hub_db import get_src_db

                return get_src_db().name
            """),
        encoding="utf-8",
    )

    driver = tmp_path / "spawn_driver.py"
    driver.write_text(
        textwrap.dedent("""
            import concurrent.futures
            import importlib
            import multiprocessing
            import os
            from pathlib import Path


            def allow_restricted_semaphore_query():
                original_sysconf = getattr(os, "sysconf", None)
                if original_sysconf is None:
                    return
                try:
                    original_sysconf("SC_SEM_NSEMS_MAX")
                except PermissionError:
                    def sysconf(name):
                        if name == "SC_SEM_NSEMS_MAX":
                            return 256
                        return original_sysconf(name)

                    os.sysconf = sysconf


            def main():
                import biothings.hub  # noqa: F401
                from plugins.nodenorm.worker import _configure_sqlite_tmpdir

                test_config = importlib.import_module(os.environ["HUB_CONFIG"])
                allow_restricted_semaphore_query()
                context = multiprocessing.get_context("spawn")
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=1,
                    mp_context=context,
                ) as executor:
                    configured_tmpdir = executor.submit(
                        _configure_sqlite_tmpdir
                    ).result(timeout=20)
                    source_db_name = executor.submit(
                        test_config.source_db_name
                    ).result(timeout=20)

                expected_tmpdir = (
                    Path(test_config.DATA_ARCHIVE_ROOT) / "sqlite_tmp"
                ).resolve()
                assert configured_tmpdir == expected_tmpdir
                assert source_db_name == test_config.DATA_SRC_DATABASE


            if __name__ == "__main__":
                main()
            """),
        encoding="utf-8",
    )

    environment = os.environ.copy()
    python_path = [str(tmp_path), str(REPOSITORY_ROOT)]
    if environment.get("PYTHONPATH"):
        python_path.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_path)
    environment["HUB_CONFIG"] = config_module_name
    environment.pop("SQLITE_TMPDIR", None)

    result = subprocess.run(
        [sys.executable, str(driver)],
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )

    assert result.returncode == 0, (
        "spawned NodeNorm worker failed to bootstrap Hub configuration:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
