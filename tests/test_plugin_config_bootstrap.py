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
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).parents[1] / "plugins"
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
    assert imported.index(BOOTSTRAP_MODULE) < imported.index(
        relative_imports[0]
    ), (
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
