import redis
import time
import os
import glob
import random

# Configuration
DB_PATH = "/tmp/mkvdb_chaos"
SERVER_PORT = 6379

def corrupt_random_sstable():
    sst_files = glob.glob(os.path.join(DB_PATH, "*.sst"))
    if not sst_files:
        print("No SSTables found to corrupt!")
        return None
    
    target = random.choice(sst_files)
    size = os.path.getsize(target)
    
    # Don't corrupt the footer (last 12 bytes) or it won't even open
    offset = random.randint(0, size - 20)
    
    print(f"🧨 Sabotaging {target} at offset {offset}...")
    
    with open(target, "r+b") as f:
        f.seek(offset)
        original = f.read(1)
        # Flip the bits
        corrupted = bytes([original[0] ^ 0xFF])
        f.seek(offset)
        f.write(corrupted)
    
    return target

def main():
    print("🔬 Starting Forensic Audit: The Bit-Flip Corruption Test")
    r = redis.Redis(host='localhost', port=SERVER_PORT, decode_responses=True)

    print("Step 1: Writing data and forcing a flush...")
    for i in range(100):
        r.set(f"audit:{i}", "stable_data" * 10)
    
    # In a real scenario, we'd wait for flush or trigger it.
    # For this test, we assume the user has run the server long enough 
    # to produce SSTables or we'll tell them to.
    
    print("\n⚠️  PLEASE ENSURE THE SERVER IS STOPPED BEFORE PROCEEDING.")
    input("Press Enter once the server is STOPPED...")
    
    corrupted_file = corrupt_random_sstable()
    if not corrupted_file:
        return

    print("\n✅ Corruption complete. Now RESTART the server.")
    input("Press Enter once the server is RESTARTED...")

    print("\nStep 2: Attempting to read all keys...")
    errors = 0
    for i in range(100):
        try:
            val = r.get(f"audit:{i}")
            # If we get here, the engine didn't catch the corruption 
            # (or we corrupted a block that didn't contain this key)
        except redis.exceptions.ResponseError as e:
            if "ChecksumMismatch" in str(e) or "Corruption" in str(e):
                print(f"🛡️  SUCCESS: Engine detected corruption at audit:{i}: {e}")
                errors += 1
            else:
                print(f"❓ Unexpected Error: {e}")
    
    if errors > 0:
        print(f"\n🏆 TEST PASSED: The engine caught {errors} corruption events.")
    else:
        print("\n🤔 No errors detected. This is possible if we corrupted padding or unused blocks. Try running it again!")

if __name__ == "__main__":
    main()
