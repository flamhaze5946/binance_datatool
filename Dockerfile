# Use debian12 as base image
FROM debian:12-slim

# Set platform
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "I am running on $BUILDPLATFORM, building for $TARGETPLATFORM"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create and set working directory
WORKDIR /app

# Copy project files
COPY . /app/

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
RUN pip install --no-cache-dir pip==23.3.1 && \
    pip install --no-cache-dir \
    aiohttp==3.9.3 \
    aiosignal==1.3.1 \
    async-timeout==4.0.3 \
    attrs==23.2.0 \
    fire==0.6.0 \
    frozenlist==1.4.1 \
    idna==3.6 \
    joblib==1.3.2 \
    multidict==6.0.5 \
    numpy==1.26.4 \
    pandas==2.2.1 \
    pyarrow==15.0.2 \
    python-dateutil==2.9.0.post0 \
    pytz==2024.1 \
    six==1.16.0 \
    termcolor==2.4.0 \
    tzdata==2024.1 \
    xmltodict==0.13.0 \
    yarl==1.9.4 \
    websockets==12.0 \
    colorama==0.4.6

# Set the entrypoint
ENTRYPOINT ["python3", "cli.py", "bmac", "start", "/app/data/"]