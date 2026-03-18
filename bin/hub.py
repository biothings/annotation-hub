#!/usr/bin/env python3

import asyncio
import os

import biothings
import config
from biothings.hub import HubServer
from biothings.utils.version import set_versions

app_folder, _src = os.path.split(os.path.split(os.path.abspath(__file__))[0])
set_versions(config, app_folder)
logging = config.logger


if __name__ == "__main__":
    logging.info(
        "Hub DB backend | %s\n"
        "Hub database   | %s",
        biothings.config.HUB_DB_BACKEND, biothings.config.DATA_HUB_DB_DATABASE
    )
    asyncio.set_running_loop(asyncio.new_event_loop())
    sources = []
    server = HubServer(sources, name=config.HUB_NAME)
    server.start()
