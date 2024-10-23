import time
from client import Client
import concurrent.futures
import os

def download_image(client, file_name):
    print(f"Starting download of {file_name}")
    download_time = client.download_image(file_name)
    print(f"Finished download of {file_name} in {download_time:.4f} seconds")
    return download_time

def run_test(num_images_per_second, duration_seconds, num_datanodes):
    print(f"\nStarting test with {num_images_per_second} images/second and {num_datanodes} datanodes")
    client = Client('localhost', 5555)
    
    # Ensure test images exist
    print("Preparing test images...")
    for i in range(num_images_per_second):
        if not os.path.exists(f'client_dir/test_image_{i}.jpg'):
            print(f"Creating test_image_{i}.jpg")
            with open(f'client_dir/test_image_{i}.jpg', 'wb') as f:
                f.write(os.urandom(1024 * 1024))  # 1MB random data
        print(f"Uploading test_image_{i}.jpg")
        client.upload_image(f'client_dir/test_image_{i}.jpg')
    
    print(f"Starting download test for {duration_seconds} seconds")
    start_time = time.time()
    end_time = start_time + duration_seconds
    
    download_times = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_images_per_second) as executor:
        iteration = 0
        while time.time() < end_time:
            print(f"Test iteration {iteration}")
            futures = [executor.submit(download_image, client, f'test_image_{i}.jpg') for i in range(num_images_per_second)]
            for future in concurrent.futures.as_completed(futures):
                download_times.append(future.result())
            time.sleep(1)
            iteration += 1
    
    avg_download_time = sum(download_times) / len(download_times)
    print(f"\nTest results for {num_images_per_second} images/second with {num_datanodes} datanodes:")
    print(f"Average download time: {avg_download_time:.4f} seconds")
    print(f"Total images downloaded: {len(download_times)}")
    print(f"Test duration: {time.time() - start_time:.2f} seconds")

if __name__ == '__main__':
    print("Starting scale test")
    for num_datanodes in [2, 4]:
        for images_per_second in [1, 5, 10]:
            run_test(images_per_second, 60, num_datanodes)
    print("Scale test completed")
