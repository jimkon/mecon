from starlette.applications import Starlette
from starlette.routing import Mount

from manual_tagging import manual_tagging_app


# combine apps ----
routes = [
    Mount('/edit_data/tags/manual', app=manual_tagging_app),
]

app = Starlette(routes=routes)