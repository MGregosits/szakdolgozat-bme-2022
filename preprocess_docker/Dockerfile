FROM python:3.8

RUN pip install pandas
RUN apt -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false update
RUN apt -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false install libcurl4-openssl-dev libssl-dev -y
RUN pip install pycurl
RUN pip install certifi

ADD preprocess.py .
