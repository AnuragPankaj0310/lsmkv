import asyncio
import os

from network.server import LsmkvServer

node = int(os.environ.get("LSMKV_NODE_INDEX", "0"))
server = LsmkvServer.from_config("config.json", node)
asyncio.run(server.serve_forever())
