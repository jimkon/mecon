from starlette.applications import Starlette
from starlette.routing import Mount

from manual_tagging import manual_tagging_app
from edit_tags import edit_tags_app


# combine apps ----
routes = [
    Mount('/edit_data/tags/manual', app=manual_tagging_app),
    Mount('/edit_data/tags/edit', app=edit_tags_app),
]

app = Starlette(routes=routes)