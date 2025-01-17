FROM apache/beam_python3.10_sdk:2.59.0
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/dataflow_pipeline.py"

