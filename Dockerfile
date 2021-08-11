FROM python:3.8-slim

# Install Python tools (git + pipenv)
RUN apt-get update && apt-get install -y git
RUN pip install pipenv

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
ADD code_schemes/*.json /app/code_schemes/
ADD configurations/ /app/configurations/
ADD src /app/src
ADD sync_coda_to_engagement_db.py /app
ADD sync_engagement_db_to_coda.py /app
ADD sync_engagement_db_to_rapid_pro.py /app
ADD engagement_db_to_analysis.py /app
