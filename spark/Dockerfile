FROM apache/spark:3.5.0

USER root

# Instalar bibliotecas Python necessárias
RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    pyarrow

WORKDIR /app

# Copiar scripts de transformação
COPY scripts/ /app/scripts/

USER 1001
