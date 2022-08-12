FROM python:3.11.0rc1-slim-bullseye

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY ./dist/cses2humio-*.tar.gz .

RUN pip3 install cses2humio-*.tar.gz && rm cses2humio*.tar.gz

ENTRYPOINT [ "cses2humio" ]