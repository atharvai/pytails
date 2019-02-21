FROM python:3.6

RUN mkdir /pytails

COPY . /pytails

WORKDIR /pytails/webapp

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "app.py"]
