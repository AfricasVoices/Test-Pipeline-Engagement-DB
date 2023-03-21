FROM python:3.8-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# If building for ARM-64 architecture, install additional dependencies which will be needed to run `pipenv sync` later.
RUN if [ $(arch) = 'aarch64' ]; then apt-get update && apt-get install -y libgeos-dev libgdal-dev build-essential; fi

# Make a directory for private credentials files
RUN mkdir /credentials

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv sync

# Copy the rest of the project
ADD .git /app/.git
ADD src /app/src
ADD *.py /app/

