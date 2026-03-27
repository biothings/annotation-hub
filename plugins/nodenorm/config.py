########################################
# DATA PLUGIN CONFIGURATION VARIABLES  #
########################################
HUB_DB_BACKEND = {
  "module": "biothings.utils.mongo",
  "uri": "mongodb://localhost:27017"
}
DATA_SRC_SERVER = "localhost"
DATA_SRC_DATABASE = "data_src_database"
DATA_ARCHIVE_ROOT = ".biothings_hub/archive"
LOG_FOLDER = ".biothings_hub/logs"
DATA_PLUGIN_FOLDER = "/home/schaffjr/workspace/plugin-development/pending.api/plugins/nodenorm"
DATA_TARGET_SERVER = "localhost"
DATA_TARGET_PORT = 27017
DATA_TARGET_DATABASE = "plugin-hub"
INDEX_CONFIG = {
  "indexer_select": {},
  "env": {
    "commandhub": {
      "host": "http://localhost:9200",
      "indexer": {
        "args": {
          "request_timeout": 300,
          "retry_on_timeout": True,
          "max_retries": 10
        }
      }
    }
  }
}
RUN_DIR = "/home/schaffjr/workspace/plugin-development/pending.api/plugins/nodenorm"
HUB_MAX_WORKERS = 16
MAX_QUEUED_JOBS = 1000
BIOTHINGS_CLI_PATH = ".biothings_hub/path"
