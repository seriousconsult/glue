import csv
import random
import string
import time

def generate_phone_number():
    """Generates a synthetic international phone number."""
    # List of common country codes
    country_codes = ['+1', '+44', '+49', '+81', '+33', '+91']
    prefix = random.choice(country_codes)
    
    # Generate a random number of digits (7 to 11) for the local number
    num_digits = random.randint(7, 11)
    local_number = ''.join(random.choices(string.digits, k=num_digits))
    
    return f"{prefix} {local_number}"

def generate_large_csv(filename, num_rows, num_columns):
    """
    Generates a large CSV file with random data.

    Args:
        filename (str): The name of the file to create.
        num_rows (int): The number of rows to generate.
        num_columns (int): The number of columns to generate.
    """
    print(f"Generating {num_rows} rows and {num_columns} columns into {filename}...")

    # We will use a buffer to write to the file in chunks to be more efficient.
    # This helps reduce the number of system calls.
    buffer_size = 10000  # Write in chunks of 10,000 rows
    progress_interval = 100000 # Report progress every 100,000 rows

    start_time = time.time()
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)

            # Generate and write a header row
            header = [f"column_{i+1}" for i in range(num_columns)]
            # Add a dedicated phone number column
            header.append("phone_number")
            writer.writerow(header)

            buffer = []
            for i in range(num_rows):
                row = []
                for j in range(num_columns):
                    # Generate a mix of data types for variety
                    if j % 3 == 0:
                        row.append(random.randint(1000, 99999)) # Integer
                    elif j % 3 == 1:
                        row.append(''.join(random.choices(string.ascii_lowercase, k=10))) # String
                    else:
                        row.append(f"{random.uniform(10.0, 100.0):.2f}") # Float
                
                # Add the synthetic international phone number
                row.append(generate_phone_number())

                # Simulate a few malformed rows by adding or removing a column
                if random.random() < 0.001:  # 0.1% chance of a malformed row
                    if random.random() < 0.5:
                        row.pop()
                    else:
                        row.append("extra_data")

                buffer.append(row)

                # Write buffer to file in chunks
                if len(buffer) >= buffer_size:
                    writer.writerows(buffer)
                    buffer = []

                # Show progress at regular intervals
                if (i + 1) % progress_interval == 0:
                    elapsed_time = time.time() - start_time
                    progress_percent = (i + 1) / num_rows * 100
                    print(f"Progress: {i+1}/{num_rows} rows generated ({progress_percent:.2f}% complete) in {elapsed_time:.2f} seconds.")

            # Write any remaining rows in the buffer
            if buffer:
                writer.writerows(buffer)

        end_time = time.time()
        print(f"Successfully generated {num_rows} rows in {end_time - start_time:.2f} seconds.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Note: The number of columns is now num_columns + 1 due to the new phone number column
    generate_large_csv("large_file.csv", 5000000, 150)

