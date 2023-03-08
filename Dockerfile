FROM python:3.8-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

# ARM support
RUN apt-get update && apt-get install -y libgeos-dev libgdal-dev build-essential

# R
RUN apt-get update && apt-get install -y python3-dev r-base cmake
RUN R -e "install.packages('arm', dependencies=TRUE, repos='https://cran.rstudio.com/')"

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

