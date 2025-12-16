from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from main_app import main_app
from datasets_app import datasets_app
from dataflow_app import dataflow_app
from monzo_app import monzo_app

async def redirect_to_menu(request):
    return RedirectResponse(url='/home')


# combine apps ----
routes = [
    Route('/', endpoint=redirect_to_menu),
    Mount('/home', app=main_app),
    Mount('/datasets', app=datasets_app),
    Mount('/data', app=dataflow_app),
    Mount('/auth/monzo', app=monzo_app),
]

app = Starlette(routes=routes)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='localhost', port=8000)