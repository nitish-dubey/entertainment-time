"""
Run all backend services together:
1. FastAPI server
2. Kafka consumer
3. Leaderboard scheduler

Usage:
    python run_all.py
"""
import subprocess
import sys
import signal
import time
from typing import List

processes: List[subprocess.Popen] = []


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\n\nShutting down all services...")
    for proc in processes:
        proc.terminate()
    
    # Wait for processes to terminate
    time.sleep(2)
    
    # Force kill if still running
    for proc in processes:
        if proc.poll() is None:
            proc.kill()
    
    print("All services stopped.")
    sys.exit(0)


def main():
    """Start all services."""
    global processes
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 60)
    print("Starting EntertainmentTime Backend Services")
    print("=" * 60)

    # 1. Start FastAPI server
    print("\n[1/5] Starting FastAPI server...")
    api_process = subprocess.Popen(
        ["uvicorn", "app.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(api_process)
    print("✓ FastAPI server started on http://localhost:8000")
    
    # 2. Start Kafka consumer
    print("\n[2/5] Starting Kafka consumer...")
    consumer_process = subprocess.Popen(
        [sys.executable, "-m", "app.consumers.video_consumer"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(consumer_process)
    print("✓ Kafka consumer started")

    # 3. Start leaderboard scheduler
    print("\n[3/5] Starting leaderboard scheduler...")
    scheduler_process = subprocess.Popen(
        [sys.executable, "-m", "app.consumers.leaderboard_scheduler"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(scheduler_process)
    print("✓ Leaderboard scheduler started")

    # 4. Start aggregation scheduler
    print("\n[4/5] Starting aggregation scheduler...")
    aggregation_process = subprocess.Popen(
        [sys.executable, "-m", "app.consumers.aggregation_scheduler"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(aggregation_process)
    print("✓ Aggregation scheduler started")

    # 5. Start transcoding worker
    print("\n[5/5] Starting transcoding worker...")
    transcoding_process = subprocess.Popen(
        [sys.executable, "-m", "app.consumers.transcoding_worker"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    processes.append(transcoding_process)
    print("✓ Transcoding worker started")

    print("\n" + "=" * 60)
    print("All services running! Press Ctrl+C to stop.")
    print("=" * 60)
    print("\nAPI Server: http://localhost:8000")
    print("API Docs: http://localhost:8000/docs")
    print("\n")
    
    # Keep main process alive
    try:
        while True:
            time.sleep(1)
            
            # Check if any process died
            for i, proc in enumerate(processes):
                if proc.poll() is not None:
                    print(f"\n⚠️  Process {i+1} died unexpectedly!")
                    # Print last output
                    if proc.stdout:
                        output = proc.stdout.read()
                        if output:
                            print(f"Last output:\n{output}")
                    signal_handler(None, None)
                    
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == '__main__':
    main()
