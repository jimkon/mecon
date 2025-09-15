import logging
import traceback

from shiny import App, Inputs, Outputs, Session, render, ui

from mecon.app import shiny_app


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


app_ui = shiny_app.app_ui_factory(
    ui.card(
        ui.h3('Menu'),
        ui.card(ui.output_text('links_output_text')),
    )
)


def server(input: Inputs, output: Outputs, session: Session):
    @render.text
    def links_output_text():
        markdown_text = ""
        dataset = shiny_app.get_working_dataset()
        links = dataset.settings.get('links', {})

        if len(links) == 0:
            return "No links found in dataset settings"

        for link_category, link_spec in links.items():
            markdown_text += f"### {link_category}\n"
            for link_name, link_url in link_spec.items():
                encode_url = link_url.replace(' ', '%20')
                logging.info(f"Link: {link_name} -> {encode_url}")
                markdown_text += f"* [{link_name}]({encode_url})\n"

        ui.insert_ui(
            ui=ui.markdown(markdown_text),
            selector='#links_output_text',
            where='beforeEnd'
        )
        return 'links'
      

main_app = App(app_ui, server)

