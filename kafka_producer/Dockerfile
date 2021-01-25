FROM python:latest
ADD producer.py /
RUN pip install kafka-python pandas pandavro fastavro
CMD ["python", "producer.py"]