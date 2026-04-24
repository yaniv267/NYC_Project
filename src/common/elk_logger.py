import socket
import json

def log_to_elk(message):
    gelf_msg = {
        "version": "1.1",
        "host": "spark_dev_env",
        "short_message": message,
        "level": 6  # 6 = INFO
    }
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(gelf_msg).encode('utf-8'), ("logstash", 12201))
    except Exception as e:
        print(f"Failed to send log to ELK: {e}")