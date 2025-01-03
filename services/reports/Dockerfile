FROM python:3.11.7-slim

RUN apt-get update
RUN apt-get --reinstall install bash-completion

RUN mkdir mecon_dir

ENV MECON_ROOT_DIRPATH=~/mecon_dir

#WORKDIR mecon_dir
#
#
#COPY ./requirements.txt ./requirements.txt
#COPY ./services/reports/requirements.txt ./requirements_app.txt
#
#RUN pip install --upgrade pip && pip install -r requirements.txt && pip install -r requirements_app.txt
#
#WORKDIR mecon_pgk
#COPY ./mecon ./mecon
#COPY ./setup.py ./setup.py
#RUN  pip install -e .
#
#WORKDIR ../reports_app
#COPY ./services/reports/ ./
##COPY ./services/reports/setting_docker.json ./settings.json
#
## todo instead of COPY, use ADD/VOLUME
#COPY ./datasets/ ./datasets
#
#CMD shiny run --host 0.0.0.0
CMD ["/bin/sh", "-c", "bash"]

