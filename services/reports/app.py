from starlette.applications import Starlette
from starlette.responses import RedirectResponse
from starlette.routing import Mount, Route

from compare_tags import compare_tags_app
from reports_menu import reports_menu_app
from tag_info import tag_info_app
from tags_info import tags_info_app


async def redirect_to_menu(request):
    return RedirectResponse(url='/reports/menu/')


routes = [
    Route('/reports/', endpoint=redirect_to_menu),
    Mount('/reports/menu/', app=reports_menu_app),
    Mount('/reports/tags/', app=tag_info_app),
    Mount('/reports/tags_info/', app=tags_info_app),
    Mount('/reports/compare/', app=compare_tags_app),
]

app = Starlette(routes=routes)
