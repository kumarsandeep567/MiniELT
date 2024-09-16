FROM python:3.11-slim

RUN apt-get update && apt-get install -y postgresql-client-15

COPY ./elt_script.py .

CMD ["python", "elt_script.py"]