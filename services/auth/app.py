from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from callbacks import handle_callback

from truelayer_auth_app import auth_app as tl_auth_app
from monzo_auth_app import auth_app as monzo_auth_app
from trading212_app import app as trd212_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/auth/truelayer')


# combine apps ----
routes = [
    Route('/', endpoint=redirect_to_menu),
    Route('/auth', endpoint=redirect_to_menu),
    Route('/auth/callback/{provider}', endpoint=handle_callback, methods=["GET"]),
    Mount('/auth/truelayer', app=tl_auth_app),
    Mount('/auth/monzo', app=monzo_auth_app),
    Mount('/auth/trd212', app=trd212_app),
]

app = Starlette(routes=routes)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='localhost', port=8003)
