FROM quay.io/astronomer/astro-runtime:12.7.1
# Add these lines before installing dependencies
ENV PIP_DEFAULT_TIMEOUT=1000
ENV PIP_RETRIES=5

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt