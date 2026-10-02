# `biothings.config` is not defined by the `biothings` package itself; importing
# `biothings.hub` is what installs it and fills in the `biothings.utils.hub_db`
# placeholders (`get_src_db()` raises NotImplementedError until then). The hub
# process has already done that, but the uploader's worker pool uses the "spawn"
# start method, so every child is a fresh interpreter that must bootstrap it
# before the modules below run `from biothings import config` -- otherwise the
# children die with ImportError while unpickling their task and the pool breaks.
import biothings.hub  # noqa: F401 # pylint: disable=unused-import

from .dumper import NodeNormDumper  # noqa # pylint: disable=unused-import
from .uploader import NodeNormUploader  # noqa # pylint: disable=unused-import
