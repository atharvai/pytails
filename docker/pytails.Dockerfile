FROM python:3.6

RUN mkdir /pytails

COPY . /pytails

WORKDIR /pytails

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "pytails.py"]
