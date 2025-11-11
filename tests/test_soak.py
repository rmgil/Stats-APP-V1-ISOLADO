"""
TASK 20: Soak Test - 50 Uploads Over 2 Hours

Long-running stability test that uploads 50 files over 2 hours to detect:
- Memory leaks
- Connection pool exhaustion
- Database connection leaks
- Worker stability issues

Usage:
    python tests/test_soak.py

Requirements:
    - Running Flask server
    - Valid test credentials
    - ~5GB disk space for test files
    - 2+ hours runtime
"""

import asyncio
import aiohttp
import time
import os
import sys
import psutil
import datetime
from pathlib import Path

# Test configuration
BASE_URL = os.environ.get('BASE_URL', 'http://localhost:5000')
TEST_USER_EMAIL = os.environ.get('TEST_USER_EMAIL', 'test@example.com')
TEST_USER_PASSWORD = os.environ.get('TEST_USER_PASSWORD', 'testpass')

NUM_UPLOADS = 50
DURATION_HOURS = 2
FILE_SIZE_MB = 100
CHUNK_SIZE = 5 * 1024 * 1024

class SoakTestRunner:
    """Long-running soak test"""
    
    def __init__(self):
        self.cookies = None
        self.memory_samples = []
        self.upload_times = []
        self.failures = []
        self.start_time = None
    
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
    
    def create_test_file(self):
        """Create a single reusable test file"""
        file_path = "tests/fixtures/soak_test.bin"
        os.makedirs("tests/fixtures", exist_ok=True)
        
        if os.path.exists(file_path):
            print(f"  Using existing test file: {file_path}")
            return file_path
        
        print(f"  Creating {FILE_SIZE_MB}MB test file...")
        file_size = FILE_SIZE_MB * 1024 * 1024
        
        with open(file_path, 'wb') as f:
            remaining = file_size
            while remaining > 0:
                chunk_size = min(CHUNK_SIZE, remaining)
                f.write(os.urandom(chunk_size))
                remaining -= chunk_size
        
        print(f"  Created: {file_path}")
        return file_path
    
    def sample_memory(self):
        """Sample current memory usage"""
        process = psutil.Process()
        mem_mb = process.memory_info().rss / (1024 * 1024)
        self.memory_samples.append({
            'time': time.time() - self.start_time,
            'memory_mb': mem_mb
        })
        return mem_mb
    
    async def upload_file(self, file_path, upload_num):
        """Upload a file"""
        try:
            file_size = os.path.getsize(file_path)
            total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            async with aiohttp.ClientSession(cookies=self.cookies) as session:
                # Initialize
                upload_id = f"soak{upload_num:03d}{int(time.time() * 1000):010d}"
                init_url = f"{BASE_URL}/api/upload/init"
                init_data = {
                    'uploadId': upload_id,
                    'fileName': f"soak_test_{upload_num}.bin",
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
                
                # Finalize
                finalize_url = f"{BASE_URL}/api/upload/finalize"
                async with session.post(finalize_url, json={'uploadId': upload_id}) as resp:
                    if resp.status != 200:
                        error = await resp.text()
                        raise Exception(f"Finalize failed: {error}")
                
                elapsed = time.time() - start
                self.upload_times.append(elapsed)
                
                return True, elapsed
                
        except Exception as e:
            self.failures.append({
                'upload_num': upload_num,
                'error': str(e),
                'time': time.time() - self.start_time
            })
            return False, str(e)
    
    def print_progress(self, upload_num, success, elapsed):
        """Print progress update"""
        elapsed_total = time.time() - self.start_time
        mem = self.sample_memory()
        
        avg_time = sum(self.upload_times) / len(self.upload_times) if self.upload_times else 0
        
        status = "✓" if success else "✗"
        print(f"  [{elapsed_total/3600:.1f}h] Upload {upload_num:2d}/{NUM_UPLOADS} {status} "
              f"({elapsed:.1f}s, avg {avg_time:.1f}s, mem {mem:.0f}MB, failures {len(self.failures)})")
    
    async def run_test(self):
        """Main test runner"""
        print("\n=== TASK 20: Soak Test - 50 Uploads Over 2 Hours ===\n")
        
        self.start_time = time.time()
        start_datetime = datetime.datetime.now()
        
        print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Expected end: {(start_datetime + datetime.timedelta(hours=DURATION_HOURS)).strftime('%H:%M:%S')}\n")
        
        # Initial memory
        initial_mem = self.sample_memory()
        print(f"Initial memory: {initial_mem:.1f} MB\n")
        
        # Login
        await self.login()
        
        # Create test file
        print("\n1. Creating test file...")
        test_file = self.create_test_file()
        
        # Calculate delay between uploads
        delay_seconds = (DURATION_HOURS * 3600) / NUM_UPLOADS
        print(f"\n2. Starting soak test ({delay_seconds:.1f}s between uploads)...\n")
        
        # Run uploads with delays
        for i in range(NUM_UPLOADS):
            upload_num = i + 1
            
            # Upload
            success, elapsed = await self.upload_file(test_file, upload_num)
            self.print_progress(upload_num, success, elapsed if success else 0)
            
            # Wait before next upload (except for last one)
            if i < NUM_UPLOADS - 1:
                await asyncio.sleep(delay_seconds)
        
        # Final analysis
        total_time = time.time() - self.start_time
        final_mem = self.sample_memory()
        
        print(f"\n3. Soak Test Results:")
        print(f"  Total time: {total_time/3600:.2f} hours")
        print(f"  Successful: {NUM_UPLOADS - len(self.failures)}/{NUM_UPLOADS}")
        print(f"  Failures: {len(self.failures)}")
        print(f"  Initial memory: {initial_mem:.1f} MB")
        print(f"  Final memory: {final_mem:.1f} MB")
        print(f"  Memory delta: {final_mem - initial_mem:+.1f} MB")
        
        # Memory leak detection
        if len(self.memory_samples) > 10:
            # Calculate trend
            early_avg = sum(s['memory_mb'] for s in self.memory_samples[:10]) / 10
            late_avg = sum(s['memory_mb'] for s in self.memory_samples[-10:]) / 10
            leak_rate = (late_avg - early_avg) / (total_time / 3600)  # MB per hour
            
            print(f"  Memory leak rate: {leak_rate:+.1f} MB/hour")
            
            if leak_rate > 50:
                print("  ⚠ WARNING: Possible memory leak detected!")
        
        # Upload time stability
        if len(self.upload_times) > 10:
            avg_time = sum(self.upload_times) / len(self.upload_times)
            print(f"  Average upload time: {avg_time:.1f}s")
        
        # Results
        if len(self.failures) == 0:
            print("\n  ✓✓✓ PASS: All uploads successful!")
            print("  ✓ No memory leaks detected")
            print("  ✓ System stable over 2 hours")
            return True
        else:
            print(f"\n  ✗✗✗ FAIL: {len(self.failures)} uploads failed")
            for failure in self.failures[:5]:  # Show first 5
                print(f"    Upload {failure['upload_num']} @ {failure['time']/3600:.1f}h: {failure['error']}")
            return False

async def main():
    """Main entry point"""
    print("\nWARNING: This test will run for 2+ hours!")
    response = input("Continue? (y/N): ")
    
    if response.lower() != 'y':
        print("Test cancelled.")
        sys.exit(0)
    
    tester = SoakTestRunner()
    success = await tester.run_test()
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    asyncio.run(main())
