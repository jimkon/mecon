from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from monzo_auth_app.py import monzo_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/edit_data/tags/menu')


# combine apps ----
routes = [
    # Route('/edit_data/', endpoint=redirect_to_menu),
    # Route('/edit_data/tags/', endpoint=redirect_to_menu),
    Mount('/auth/monzo', app=monzo_app),
]

app = Starlette(routes=routes)
