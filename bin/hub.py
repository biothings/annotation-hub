#!/usr/bin/env python3

import asyncio
import os
import config, biothings
from biothings.utils.version import set_versions

app_folder, _src = os.path.split(os.path.split(os.path.abspath(__file__))[0])
set_versions(config, app_folder)
logging = config.logger

from prepare_multiomics import PatchError, block_unprepared_multiomics, prepare_multiomics

multiomics_ready = True
try:
    patched_files = prepare_multiomics(app_folder, allow_missing=True)
except (PatchError, OSError) as error:
    multiomics_ready = False
    logging.error(
        "Multiomics preparation failed; Multiomics sources are disabled for this "
        "hub process while other sources continue: %s",
        error,
        extra={"notify": True},
    )
else:
    if patched_files:
        logging.info("Applied local Multiomics patches: %s", ", ".join(patched_files))


from biothings.hub import HubServer
import hub.dataload.sources


class AnnotationHubServer(HubServer):
    def configure_source_manager(self):
        # Guard before static discovery, manifest loading, and scheduling.
        if not multiomics_ready:
            for name in ("dump_manager", "upload_manager", "dataplugin_manager"):
                manager = self.managers.get(name)
                if manager is not None:
                    block_unprepared_multiomics(manager, logging)
        return super().configure_source_manager()


if __name__ == "__main__":
    logging.info("Hub DB backend: %s" % biothings.config.HUB_DB_BACKEND)
    logging.info("Hub database: %s" % biothings.config.DATA_HUB_DB_DATABASE)
    asyncio.set_event_loop(asyncio.new_event_loop())
    server = AnnotationHubServer(hub.dataload.sources, name=config.HUB_NAME)
    server.start()
