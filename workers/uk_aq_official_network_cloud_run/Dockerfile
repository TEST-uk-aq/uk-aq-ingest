FROM python:3.12-slim
WORKDIR /app
COPY workers/uk_aq_official_network_cloud_run/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY scripts /app/scripts
COPY workers/uk_aq_official_network_cloud_run/run_service.py /app/workers/uk_aq_official_network_cloud_run/run_service.py
COPY workers/uk_aq_official_network_cloud_run/run_job.py /app/workers/uk_aq_official_network_cloud_run/run_job.py
ENV PORT=8000
CMD ["python3", "/app/workers/uk_aq_official_network_cloud_run/run_service.py"]
