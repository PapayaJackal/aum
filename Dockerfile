# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy the pyproject.toml and poetry.lock files to the container
COPY pyproject.toml poetry.lock ./

# Install the dependencies
RUN poetry install --no-root

# Copy the rest of the application code
COPY aum aum

# Expose the port your app runs on
EXPOSE 8000

# Define the command to run the application
CMD ["sh", "-c", "poetry run python -m aum index index /data ; poetry run python -m aum serve index"]