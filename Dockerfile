FROM nycplanning/docker-geosupport:latest

COPY requirements.txt .
RUN pip install --upgrade pip && \
	pip install -r requirements.txt

COPY . /nyc-geocode
WORKDIR /nyc-geocode

CMD [ "python3", "main.py"]

ENV PYTHONBUFFERED 1