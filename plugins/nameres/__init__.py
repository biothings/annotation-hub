# `biothings.config` is not defined by the `biothings` package itself; importing
# `biothings.hub` is what installs it and fills in the `biothings.utils.hub_db`
# placeholders (`get_src_db()` raises NotImplementedError until then). The hub
# process has already done that, but the uploader's worker pool children may be
# fresh interpreters -- Python 3.14 defaults to the "forkserver" start method on
# Linux -- so bootstrap it before the modules below run
# `from biothings import config`.
import biothings.hub  # noqa: F401 # pylint: disable=unused-import

from .dumper import NameResDumper  # noqa # pylint: disable=unused-import
from .uploader import NameResUploader  # noqa # pylint: disable=unused-import
