from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from edit_tags import edit_tags_app
from manual_tagging import manual_tagging_app
from menu_tags import menu_tags_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/edit_data/tags/menu')


# combine apps ----
routes = [
    Route('/edit_data/', endpoint=redirect_to_menu),
    Route('/edit_data/tags/', endpoint=redirect_to_menu),
    Mount('/edit_data/tags/menu', app=menu_tags_app),
    Mount('/edit_data/tags/manual', app=manual_tagging_app),
    Mount('/edit_data/tags/edit', app=edit_tags_app),
]

app = Starlette(routes=routes)
