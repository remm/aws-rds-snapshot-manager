FROM python:3.6-alpine3.6

RUN pip install boto3

ADD snapshot-manager.py /home

CMD ["echo 0"]