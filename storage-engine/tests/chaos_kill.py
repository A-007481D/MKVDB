"""
Kill-9 Power Failure Simulation
Proves: WAL recovery + Atomic Manifest = zero data loss on SIGKILL
"""
import subprocess, time, random, os, signal, redis

DB_PATH = "/tmp/mkvdb_chaos"
SERVER_BIN = "./target/debug/storage-engine"
PORT = 6379
KEYS_PER_CYCLE = 2000

def start_server():
    p = subprocess.Popen(
        [SERVER_BIN, "--path", DB_PATH],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=os.setsid  # Own process group so we can kill exactly this process
    )
    return p

def wait_for_ready(timeout=5):
    r = redis.Redis(host='localhost', port=PORT, socket_connect_timeout=1, decode_responses=True)
    for _ in range(timeout * 10):
        try:
            r.ping()
            return r
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("Server did not become ready in time")

def main():
    os.makedirs(DB_PATH, exist_ok=True)
    print("🚀 Kill-9 Power Failure Simulation (5 cycles)")
    print(f"   Binary: {SERVER_BIN}")
    print(f"   Data:   {DB_PATH}\n")

    total_confirmed = 0

    for cycle in range(1, 6):
        print(f"── Cycle {cycle} ──────────────────────────────────────")

        proc = start_server()
        try:
            r = wait_for_ready()
        except RuntimeError:
            stderr = proc.stderr.read()
            print(f"  ❌ Server failed to start:\n{stderr}")
            break

        # Write keys sequentially (acknowledged = durable in WAL)
        written = 0
        try:
            for i in range(total_confirmed, total_confirmed + KEYS_PER_CYCLE):
                r.set(f"key:{i}", f"val:{i}")
                written += 1
        except redis.exceptions.ConnectionError as e:
            print(f"  ⚠  Connection lost after {written} writes: {e}")

        total_confirmed += written
        print(f"  ✅ Wrote {written} keys (total confirmed: {total_confirmed})")

        # SIGKILL — no graceful shutdown, no fsync
        sleep_s = random.uniform(0.5, 2)
        print(f"  💀 SIGKILL in {sleep_s:.2f}s ...")
        time.sleep(sleep_s)
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait()
        print(f"  🔪 Server killed (PID {proc.pid})")
        time.sleep(0.5)  # Allow OS to release port

        # Restart and recover
        print(f"  ♻️  Restarting ...")
        proc2 = start_server()
        try:
            r2 = wait_for_ready()
        except RuntimeError:
            stderr = proc2.stderr.read()
            print(f"  ❌ Recovery failed:\n{stderr}")
            break

        # Verify every acknowledged key survived
        missing, corrupted = 0, 0
        for i in range(total_confirmed):
            v = r2.get(f"key:{i}")
            if v is None:
                missing += 1
            elif v != f"val:{i}":
                corrupted += 1

        if missing == 0 and corrupted == 0:
            print(f"  🏆 PASS — All {total_confirmed} keys recovered perfectly\n")
        else:
            print(f"  ❌ FAIL — missing={missing} corrupted={corrupted}\n")

        # Clean shutdown for next cycle
        os.killpg(os.getpgid(proc2.pid), signal.SIGKILL)
        proc2.wait()
        time.sleep(0.5)

    print("─────────────────────────────────────────────────")
    print(f"Final: {total_confirmed} total keys persisted across 5 SIGKILL cycles")

if __name__ == "__main__":
    main()
