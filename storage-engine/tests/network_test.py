import socket
import time

def send_resp(s, cmd):
    # Convert list of strings to RESP array
    resp = f"*{len(cmd)}\r\n"
    for arg in cmd:
        resp += f"${len(arg)}\r\n{arg}\r\n"
    s.send(resp.encode())
    return s.recv(4096)

def test_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 6379))
    
    print("Testing PING...")
    res = send_resp(s, ["PING"])
    print(f"  Response: {res}")
    
    print("Testing SET...")
    res = send_resp(s, ["SET", "user:1", "Antigravity"])
    print(f"  Response: {res}")
    
    print("Testing GET...")
    res = send_resp(s, ["GET", "user:1"])
    print(f"  Response: {res}")
    
    print("Testing SCAN...")
    res = send_resp(s, ["SCAN", "user:0", "user:9"])
    print(f"  Response: {res}")
    
    s.close()

if __name__ == "__main__":
    test_server()
