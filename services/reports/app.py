from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from compare_tags import compare_tags_app
from reports_menu import reports_menu_app
from tag_info import tag_info_app
from tags_info import tags_info_app
from overall_dashboard_app import dashboard_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/reports/menu/')


routes = [
    Route('/', endpoint=redirect_to_menu),
    Route('/reports/', endpoint=redirect_to_menu),
    Mount('/reports/menu/', app=reports_menu_app),
    Mount('/reports/tags/', app=tag_info_app),
    Mount('/reports/tags_info/', app=tags_info_app),
    Mount('/reports/compare/', app=compare_tags_app),
    Mount('/reports/overall/', app=dashboard_app),
]

app = Starlette(routes=routes)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='localhost', port=8001)
