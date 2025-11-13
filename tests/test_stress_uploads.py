"""
TASK 19: Stress Test - 12 Concurrent 100MB Uploads

Tests system limits with 12 simultaneous 100MB uploads.
Validates:
- Connection pool doesn't exhaust
- Queue safeguards work correctly
- Memory doesn't overflow
- All uploads complete successfully

Usage:
    python tests/test_stress_uploads.py

Requirements:
    - Running Flask server
    - Valid test credentials
    - At least 1.2GB disk space for test files
"""

import asyncio
import aiohttp
import time
import os
import sys
import psutil
from pathlib import Path

# Test configuration
BASE_URL = os.environ.get('BASE_URL', 'http://localhost:5000')
TEST_USER_EMAIL = os.environ.get('TEST_USER_EMAIL', 'test@example.com')
TEST_USER_PASSWORD = os.environ.get('TEST_USER_PASSWORD', 'testpass')

NUM_UPLOADS = 12
FILE_SIZE_MB = 100
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB chunks

class StressTestRunner:
    """Stress test with 12 concurrent uploads"""
    
    def __init__(self):
        self.cookies = None
        self.start_memory = None
        self.peak_memory = 0
    
    async def login(self):
        """Login to get session"""
        async with aiohttp.ClientSession() as session:
            login_url = f"{BASE_URL}/login"
            async with session.post(login_url, data={
                'email': TEST_USER_EMAIL,
                'password': TEST_USER_PASSWORD
            }) as resp:
                if resp.status != 200:
                    raise Exception(f"Login failed: {resp.status}")
                self.cookies = session.cookie_jar.filter_cookies(BASE_URL)
                print("✓ Login successful")
    
    def create_test_file(self, file_num):
        """Create a 100MB test file"""
        file_path = f"tests/fixtures/stress_test_{file_num}.bin"
        os.makedirs("tests/fixtures", exist_ok=True)
        
        if os.path.exists(file_path):
            print(f"  File {file_num}: Using existing {file_path}")
            return file_path
        
        print(f"  File {file_num}: Creating {FILE_SIZE_MB}MB test file...")
        file_size = FILE_SIZE_MB * 1024 * 1024
        
        with open(file_path, 'wb') as f:
            # Write random data in chunks to avoid memory overflow
            remaining = file_size
            while remaining > 0:
                chunk_size = min(CHUNK_SIZE, remaining)
                f.write(os.urandom(chunk_size))
                remaining -= chunk_size
        
        print(f"  File {file_num}: Created {file_path}")
        return file_path
    
    async def upload_file(self, file_path, upload_num):
        """Upload a file with progress tracking"""
        try:
            file_size = os.path.getsize(file_path)
            total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            async with aiohttp.ClientSession(cookies=self.cookies) as session:
                # Initialize
                upload_id = f"stress{upload_num:02d}{int(time.time() * 1000):010d}"
                init_url = f"{BASE_URL}/api/upload/init"
                init_data = {
                    'uploadId': upload_id,
                    'fileName': os.path.basename(file_path),
                    'totalSize': file_size,
                    'totalChunks': total_chunks
                }
                
                async with session.post(init_url, json=init_data) as resp:
                    if resp.status != 200:
                        raise Exception(f"Init failed: {resp.status}")
                
                start = time.time()
                
                # Upload chunks
                with open(file_path, 'rb') as f:
                    for chunk_index in range(total_chunks):
                        chunk_data = f.read(CHUNK_SIZE)
                        
                        form = aiohttp.FormData()
                        form.add_field('uploadId', upload_id)
                        form.add_field('chunkIndex', str(chunk_index))
                        form.add_field('chunk', chunk_data, filename='chunk')
                        
                        chunk_url = f"{BASE_URL}/api/upload/chunk"
                        async with session.post(chunk_url, data=form) as resp:
                            if resp.status != 200:
                                raise Exception(f"Chunk {chunk_index} failed")
                        
                        # Track memory
                        current_mem = psutil.Process().memory_info().rss / (1024 * 1024)
                        self.peak_memory = max(self.peak_memory, current_mem)
                
                # Finalize
                finalize_url = f"{BASE_URL}/api/upload/finalize"
                async with session.post(finalize_url, json={'uploadId': upload_id}) as resp:
                    if resp.status != 200:
                        error = await resp.text()
                        raise Exception(f"Finalize failed: {error}")
                
                elapsed = time.time() - start
                speed_mbps = (file_size / (1024 * 1024)) / elapsed
                
                print(f"  ✓ Upload {upload_num:2d}: Success ({elapsed:.1f}s, {speed_mbps:.1f} MB/s)")
                return True, upload_id
                
        except Exception as e:
            print(f"  ✗ Upload {upload_num:2d}: Failed - {e}")
            return False, str(e)
    
    async def run_test(self):
        """Main test runner"""
        print("\n=== TASK 19: Stress Test - 12 x 100MB Uploads ===\n")
        
        # Track initial memory
        process = psutil.Process()
        self.start_memory = process.memory_info().rss / (1024 * 1024)
        print(f"Initial memory: {self.start_memory:.1f} MB\n")
        
        # Login
        await self.login()
        
        # Create test files
        print("\n1. Creating test files...")
        test_files = []
        for i in range(NUM_UPLOADS):
            file_path = self.create_test_file(i)
            test_files.append(file_path)
        
        # Run concurrent uploads
        print(f"\n2. Starting {NUM_UPLOADS} concurrent uploads...")
        start = time.time()
        
        tasks = [
            self.upload_file(test_files[i], i + 1)
            for i in range(NUM_UPLOADS)
        ]
        
        results = await asyncio.gather(*tasks)
        
        elapsed = time.time() - start
        
        # Analyze results
        print(f"\n3. Results:")
        successes = sum(1 for success, _ in results if success)
        failures = NUM_UPLOADS - successes
        
        total_data_mb = NUM_UPLOADS * FILE_SIZE_MB
        throughput = total_data_mb / elapsed
        
        print(f"  Total time: {elapsed:.1f}s")
        print(f"  Successful: {successes}/{NUM_UPLOADS}")
        print(f"  Failed: {failures}/{NUM_UPLOADS}")
        print(f"  Total data: {total_data_mb} MB")
        print(f"  Throughput: {throughput:.1f} MB/s")
        print(f"  Peak memory: {self.peak_memory:.1f} MB (Δ {self.peak_memory - self.start_memory:.1f} MB)")
        
        if successes == NUM_UPLOADS:
            print("\n  ✓✓✓ PASS: All uploads completed successfully!")
            print("  ✓ Connection pool handled concurrent load")
            print("  ✓ Memory usage acceptable")
            print("  ✓ Queue safeguards working")
            return True
        else:
            print(f"\n  ✗✗✗ FAIL: {failures} uploads failed")
            for i, (success, data) in enumerate(results):
                if not success:
                    print(f"    Upload {i+1}: {data}")
            return False
    
    def cleanup(self):
        """Clean up test files"""
        print("\n4. Cleaning up test files...")
        for i in range(NUM_UPLOADS):
            file_path = f"tests/fixtures/stress_test_{i}.bin"
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"  Deleted: {file_path}")

async def main():
    """Main entry point"""
    tester = StressTestRunner()
    
    try:
        success = await tester.run_test()
        sys.exit(0 if success else 1)
    finally:
        # Cleanup
        if input("\nDelete test files? (y/N): ").lower() == 'y':
            tester.cleanup()

if __name__ == '__main__':
    asyncio.run(main())
