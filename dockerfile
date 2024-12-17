FROM python:3.11

WORKDIR /code

COPY pyproject.toml uv.lock .python-version /code/

# Install PDM
RUN pip install uv

COPY logs.py main.py node.py /code/

# Set environment variable to disable output buffering
ENV PYTHONUNBUFFERED=1

# STOPS HASH FUNCTION FROM BEING RANDOM
ENV PYTHONHASHSEED=0

CMD ["uv", "run", "main.py"]
