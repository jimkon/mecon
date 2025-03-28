from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from main_app import main_app
from monzo_app import monzo_app

async def redirect_to_menu(request):
    return RedirectResponse(url='/home')


# combine apps ----
routes = [
    Route('/', endpoint=redirect_to_menu),
    Mount('/home', app=main_app),
    Mount('/auth/monzo', app=monzo_app),
]

app = Starlette(routes=routes)
