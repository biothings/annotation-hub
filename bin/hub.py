#!/usr/bin/env python3

import asyncio
import os
import config, biothings
from biothings.utils.version import set_versions

app_folder, _src = os.path.split(os.path.split(os.path.abspath(__file__))[0])
set_versions(config, app_folder)
logging = config.logger


from biothings.hub import HubServer
import hub.dataload.sources

if __name__ == "__main__":
    logging.info("Hub DB backend: %s" % biothings.config.HUB_DB_BACKEND)
    logging.info("Hub database: %s" % biothings.config.DATA_HUB_DB_DATABASE)
    asyncio.set_event_loop(asyncio.new_event_loop())
    server = HubServer(hub.dataload.sources, name=config.HUB_NAME)
    server.start()
