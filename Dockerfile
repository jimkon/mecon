FROM python:3.8.16-slim

WORKDIR  /app
COPY . .

RUN pip install -r requirements.txt
RUN pip install -e .

#CMD ["python", "-m", "app.py"]
#CMD ["flask", "--app", "server", "run"]
CMD [ "python", "-m" , "flask", "run", "--host=0.0.0.0"]

