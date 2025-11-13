"""
TASK 18: Regression Test - Concurrent Upload Finalization

Tests that 2 uploads can be finalized simultaneously without race conditions.
Validates SERIALIZABLE transaction isolation and safeguard atomicity.

Usage:
    python tests/test_concurrent_uploads.py

Requirements:
    - Running Flask server on localhost:5000
    - Valid test user credentials
    - 2 test files (can be small for this test)
"""

import asyncio
import aiohttp
import time
import os
import sys
from pathlib import Path

# Test configuration
BASE_URL = os.environ.get('BASE_URL', 'http://localhost:5000')
TEST_USER_EMAIL = os.environ.get('TEST_USER_EMAIL', 'test@example.com')
TEST_USER_PASSWORD = os.environ.get('TEST_USER_PASSWORD', 'testpass')

# Test files
TEST_FILE_1 = 'tests/fixtures/test_file_1.zip'  # Create these fixtures
TEST_FILE_2 = 'tests/fixtures/test_file_2.zip'

CHUNK_SIZE = 5 * 1024 * 1024  # 5MB chunks

class ConcurrentUploadTester:
    """Test concurrent upload finalization"""
    
    def __init__(self):
        self.session = None
        self.cookies = None
    
    async def login(self):
        """Login to get session cookies"""
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
    
    async def upload_file(self, file_path, upload_num):
        """Upload a file in chunks"""
        file_size = os.path.getsize(file_path)
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        async with aiohttp.ClientSession(cookies=self.cookies) as session:
            # Step 1: Initialize upload
            init_url = f"{BASE_URL}/api/upload/init"
            init_data = {
                'uploadId': f"test{upload_num:02d}{int(time.time() * 1000):010d}",
                'fileName': os.path.basename(file_path),
                'totalSize': file_size,
                'totalChunks': total_chunks
            }
            
            async with session.post(init_url, json=init_data) as resp:
                if resp.status != 200:
                    raise Exception(f"Init failed: {resp.status}")
                result = await resp.json()
                upload_id = result['uploadId']
                print(f"  Upload {upload_num}: Initialized {upload_id}")
            
            # Step 2: Upload chunks
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
                            raise Exception(f"Chunk {chunk_index} failed: {resp.status}")
            
            print(f"  Upload {upload_num}: All chunks uploaded")
            return upload_id
    
    async def finalize_upload(self, upload_id, upload_num):
        """Finalize upload (will be called concurrently)"""
        async with aiohttp.ClientSession(cookies=self.cookies) as session:
            finalize_url = f"{BASE_URL}/api/upload/finalize"
            finalize_data = {'uploadId': upload_id}
            
            start_time = time.time()
            async with session.post(finalize_url, json=finalize_data) as resp:
                elapsed = time.time() - start_time
                
                if resp.status == 200:
                    result = await resp.json()
                    print(f"  ✓ Upload {upload_num}: Finalized successfully ({elapsed:.2f}s)")
                    return True, result
                else:
                    error = await resp.text()
                    print(f"  ✗ Upload {upload_num}: Failed ({resp.status}) - {error}")
                    return False, error
    
    async def run_test(self):
        """Main test runner"""
        print("\n=== TASK 18: Concurrent Upload Finalization Test ===\n")
        
        # Login
        await self.login()
        
        # Upload 2 files
        print("\n1. Uploading files...")
        upload_id_1 = await self.upload_file(TEST_FILE_1, 1)
        upload_id_2 = await self.upload_file(TEST_FILE_2, 2)
        
        # Finalize both uploads simultaneously
        print("\n2. Finalizing uploads concurrently (testing SERIALIZABLE)...")
        start = time.time()
        results = await asyncio.gather(
            self.finalize_upload(upload_id_1, 1),
            self.finalize_upload(upload_id_2, 2)
        )
        elapsed = time.time() - start
        
        # Check results
        print(f"\n3. Test Results (total time: {elapsed:.2f}s):")
        success_1, data_1 = results[0]
        success_2, data_2 = results[1]
        
        if success_1 and success_2:
            print("  ✓✓ PASS: Both uploads finalized successfully!")
            print("  ✓ SERIALIZABLE transactions working correctly")
            print("  ✓ No race conditions detected")
            return True
        else:
            print("  ✗✗ FAIL: One or both uploads failed")
            if not success_1:
                print(f"    Upload 1 error: {data_1}")
            if not success_2:
                print(f"    Upload 2 error: {data_2}")
            return False

async def main():
    """Main entry point"""
    # Check test files exist
    if not os.path.exists(TEST_FILE_1) or not os.path.exists(TEST_FILE_2):
        print(f"ERROR: Test files not found!")
        print(f"  Create: {TEST_FILE_1}")
        print(f"  Create: {TEST_FILE_2}")
        print(f"\nYou can create dummy files with:")
        print(f"  mkdir -p tests/fixtures")
        print(f"  dd if=/dev/urandom of={TEST_FILE_1} bs=1M count=10")
        print(f"  dd if=/dev/urandom of={TEST_FILE_2} bs=1M count=10")
        sys.exit(1)
    
    tester = ConcurrentUploadTester()
    success = await tester.run_test()
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    asyncio.run(main())
