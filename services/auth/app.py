from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from truelayer_auth_app import auth_app as tl_auth_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/auth/truelayer')


# combine apps ----
routes = [
    Route('/', endpoint=redirect_to_menu),
    Route('/auth', endpoint=redirect_to_menu),
    Mount('/auth/truelayer', app=tl_auth_app),
]

app = Starlette(routes=routes)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='localhost', port=8003)
