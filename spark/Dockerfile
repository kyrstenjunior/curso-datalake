FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    pyarrow \
    requests

WORKDIR /app

COPY scripts/ /app/scripts/

USER 1001