FROM python:3.11-slim
RUN pip install --no-cache-dir arpeio-mcp uvicorn starlette
COPY app.py .
EXPOSE 7860
CMD ["python", "app.py"]
