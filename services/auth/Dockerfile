FROM python:3.11.7-slim

RUN apt-get update

WORKDIR mecon

COPY ./requirements.txt ./requirements.txt
COPY ./services/auth/requirements.txt ./requirements_shiny.txt

RUN python -m pip install --upgrade pip && pip install -r requirements.txt && pip install -r requirements_shiny.txt

COPY ./mecon ./mecon
COPY ./setup.py ./setup.py
RUN  pip install -e .

COPY ./services/auth/ ./services/auth/

CMD ["shiny", "run", "--host", "0.0.0.0", "--port", "8000", "./services/auth/app.py"]
