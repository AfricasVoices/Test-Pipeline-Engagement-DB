FROM python:3.8-slim

# Install Python tools (git + pdm)
RUN apt-get update && apt-get install -y git
RUN pip install -U pip setuptools wheel
RUN pip install pdm

# If building for ARM-64 architecture, install additional dependencies which will be needed to run `pdm sync` later.
RUN if [ $(arch) = 'aarch64' ]; then apt-get update && apt-get install -y libgeos-dev libgdal-dev build-essential; fi

# R
RUN apt-get update && apt-get install -y python3-dev r-base cmake
# Install the arm package in R, which is an R package for applied regression modeling
RUN R -e "install.packages('arm', dependencies=TRUE, repos='https://cran.rstudio.com/')"
# Install the mice package in R, which is a missing value imputation package
RUN R -e "install.packages('mice', dependencies=TRUE, repos='https://cran.rstudio.com/')"

# Make a directory for private credentials files
RUN mkdir /credentials

# Make a directory for intermediate data
RUN mkdir /data

# Set working directory
WORKDIR /app

# Install project dependencies.
ADD pyproject.toml /app
ADD pdm.lock /app
ADD README.md /app
# RUN pdm lock --check && pdm sync
RUN pdm install --no-lock --no-editable

# Copy the rest of the project
ADD .git /app/.git
ADD src /app/src
ADD *.py /app/

