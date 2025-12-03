# Usa uma imagem oficial do Jupyter que já vem com Pandas, Scikit-Learn, etc.
FROM jupyter/scipy-notebook:latest

# Troca para usuário root para poder instalar pacotes novos
USER root

# Instala as bibliotecas necessárias para conectar no Banco e no MinIO
RUN pip install --no-cache-dir \
    mlflow \
    boto3 \
    psycopg2-binary \
    sqlalchemy \
    minio \
    requests \
    paho-mqtt

# (Opcional) Se precisar de visualização extra
# RUN pip install matplotlib seaborn plotly

# Volta para o usuário padrão do Jupyter (jovyan) para evitar erros de permissão
USER ${NB_UID}