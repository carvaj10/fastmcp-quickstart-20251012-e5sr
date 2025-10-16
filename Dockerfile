FROM 342547628772.dkr.ecr.us-east-1.amazonaws.com/fastmcp-prd-base-images:mcp-base-python3.12

# Instalar dependencias del sistema para ODBC y SQL Server
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Configurar variables de entorno para ODBC
ENV ODBCINI=/etc/odbc.ini
ENV ODBCSYSINI=/etc

# Copiar código de la aplicación
COPY . .

# Instalar FastMCP CLI para inspección
RUN uv pip install --system "fastmcp==2.12.3"

# Instalar dependencias de Python desde pyproject.toml
RUN uv pip install --system -r pyproject.toml

# Configurar variables de entorno de FastMCP Cloud
ARG HOME
ARG FASTMCP_CLOUD_URL
ARG FASTMCP_CLOUD_GIT_COMMIT_SHA
ARG FASTMCP_CLOUD_GIT_REPO
ENV HOME=$HOME
ENV FASTMCP_CLOUD_URL=$FASTMCP_CLOUD_URL
ENV FASTMCP_CLOUD_GIT_COMMIT_SHA=$FASTMCP_CLOUD_GIT_COMMIT_SHA
ENV FASTMCP_CLOUD_GIT_REPO=$FASTMCP_CLOUD_GIT_REPO

# Inspeccionar herramientas MCP (continuar build si falla)
RUN fastmcp inspect -f fastmcp -o /tmp/server-info.json "/app/echo.py" || echo '{"error": "Failed to inspect MCP tools"}' > /tmp/server-info.json

# Puerto por defecto para Lambda Web Adapter
EXPOSE 8080
