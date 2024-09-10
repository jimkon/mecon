FROM python:3.11.7-slim

RUN apt-get update
RUN apt-get --reinstall install bash-completion



COPY ./requirements.txt ./requirements.txt
COPY ./services/main_shiny/requirements.txt ./requirements_shiny.txt

RUN pip install --upgrade pip && pip install -r requirements.txt && pip install -r requirements_shiny.txt

WORKDIR mecon_pgk
COPY ./mecon ./mecon
COPY ./setup.py ./setup.py
RUN  pip install -e .

WORKDIR ../main_app
COPY ./services/main_shiny/ ./

COPY ./datasets/ ./datasets

CMD shiny run --host 0.0.0.0

