FROM apache/beam_python3.10_sdk:2.61.0
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/app/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/mysqldataflowcloudsql.py"

