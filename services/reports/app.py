from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.staticfiles import StaticFiles

from shiny import App, ui

from tag_info import tag_info_app

# first starlette app, just serves static files ----
app_static = StaticFiles(directory=".")

# shiny app ----
app_shiny = App(ui.page_fluid("hello from shiny!"), None)


# combine apps ----
routes = [
    Mount('/reports/tags/', app=tag_info_app),
]

app = Starlette(routes=routes)