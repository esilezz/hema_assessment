#Deriving the latest base image
FROM python:latest

# Install OpenJDK 8
RUN \
    apt-get update && \
    apt-get install -y openjdk-11-jre && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y python3 python3-pip

COPY requirements.txt .

RUN pip install pyspark


WORKDIR /usr/app/src

COPY main.py .
COPY . /usr/app/src


CMD [ "python", "main.py"]