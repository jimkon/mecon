FROM python:3.12-slim

RUN apt-get update

RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install future
COPY . .

CMD [ "flask", "--debug", "run", "--host=0.0.0.0"]
