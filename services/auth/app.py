from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from auth_app import auth_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/auth/')


# combine apps ----
routes = [
    Route('/auth', endpoint=redirect_to_menu),
    Mount('/auth/', app=auth_app),
]

app = Starlette(routes=routes)
