# Get the python version
FROM python:3.8.10-slim

# Create a folder for the app
RUN mkdir -p /usr/src/eth-block-monitor

# Copy all the files to the directory
COPY src /usr/src/eth-block-monitor/src
COPY requirements.txt /usr/src/eth-block-monitor/requirements.txt

# Create a folder for the database
RUN mkdir -p /usr/src/eth-block-monitor/data

# Create a folder for the relay config
RUN mkdir -p /usr/src/eth-block-monitor/default

# Copy the relay config
COPY default/relay_config.json /usr/src/eth-block-monitor/default/relay_config.json

# Install the python requirements
RUN pip install -r /usr/src/eth-block-monitor/requirements.txt

# Define the working directory
WORKDIR /usr/src/eth-block-monitor/src

ENTRYPOINT ["python", "-u", "main.py"]