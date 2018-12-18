FROM python:3.6

# add required python modules
COPY requirements.txt /
RUN pip install -r /requirements.txt

# add this folder to the Docker image
COPY . /app
WORKDIR /root

