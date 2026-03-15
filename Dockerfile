FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer — only re-runs if requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

# Create directories for logs and data
RUN mkdir -p logs/runs data

# Expose Streamlit port
EXPOSE 8501

# Start pipeline scheduler + dashboard together
CMD ["python", "-c", "\
import subprocess, sys, signal, os;\
os.makedirs('logs', exist_ok=True);\
procs = [\
  subprocess.Popen([sys.executable, 'pipelines/pipeline.py', '--serve']),\
  subprocess.Popen([sys.executable, '-m', 'streamlit', 'run', 'dashboard/app.py', '--server.address', '0.0.0.0']),\
];\
signal.signal(signal.SIGINT, lambda *_: [p.terminate() for p in procs]);\
signal.signal(signal.SIGTERM, lambda *_: [p.terminate() for p in procs]);\
[p.wait() for p in procs]"]
