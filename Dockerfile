FROM python:3.8.16-slim

#USER admin

WORKDIR  /app
RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN pip install -e .

CMD [ "python", "-m" , "flask", "run", "--host=0.0.0.0"]

