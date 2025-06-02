FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias base
RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https unixodbc unixodbc-dev gcc g++ python3 python3-pip libssl1.1 nano

# Agregar repositorio Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl -o /etc/apt/sources.list.d/mssql-release.list https://packages.microsoft.com/config/ubuntu/18.04/prod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Configurar odbc.ini con DSN para SQL Server
RUN echo "[TestSQLServer]" > /etc/odbc.ini && \
    echo "Driver = ODBC Driver 17 for SQL Server" >> /etc/odbc.ini && \
    echo "Server = 168.205.92.17\\sqlexpress" >> /etc/odbc.ini && \
    echo "Database = ObraSeco" >> /etc/odbc.ini && \
    echo "Encrypt = no" >> /etc/odbc.ini && \
    echo "TrustServerCertificate = yes" >> /etc/odbc.ini

# Directorio de trabajo y copia de archivos
WORKDIR /app
COPY . /app

# Instalar dependencias Python
RUN pip3 install pyodbc

CMD ["bash"]

RUN pip3 install flask

EXPOSE 5000
CMD ["python3", "/app/app.py"]
