FROM python:3.11.7-slim

RUN apt-get update

WORKDIR mecon

COPY ./requirements.txt ./requirements.txt
COPY ./services/main_shiny/requirements.txt ./requirements_shiny.txt

RUN python -m pip install --upgrade pip && pip install -r requirements.txt && pip install -r requirements_shiny.txt

COPY ./mecon ./mecon
COPY ./setup.py ./setup.py
RUN  pip install -e .

COPY ./services/setup/ ./services/setup/

CMD ["python", "./services/setup/rollover_dataset.py"]
