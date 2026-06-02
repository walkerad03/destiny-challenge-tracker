FROM python:3.14

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./src /code/src
COPY ./templates /code/templates

CMD ["fastapi", "run", "src/main.py", "--port", "8000"]
