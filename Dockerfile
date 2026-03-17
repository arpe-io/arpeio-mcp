FROM python:3.11-slim
COPY . /app
WORKDIR /app
RUN pip install --no-cache-dir . uvicorn starlette
EXPOSE 7860
CMD ["python", "app.py"]
