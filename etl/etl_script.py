import subprocess
import time
import os

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host],
                check=True,
                capture_output=True,
                text=True
            )
            if "accepting connections" in result.stdout:
                print('Successfully connected to Postgres')
                return True
        except subprocess.CalledProcessError as e:
            print(f'Error connecting to Postgres: {e}')
        retries += 1
        print(f'Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})')
        time.sleep(delay_seconds)
    print('Max retries reached. Exiting')
    return False

# Check initial connection to source Postgres
if not wait_for_postgres(host='source_postgres'):
    exit(1)

print('Starting ETL Script...')

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

# Dump command
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

# Set environment variable for pg_dump
subprocess_env = os.environ.copy()
subprocess_env['PGPASSWORD'] = source_config['password']

try:
    subprocess.run(dump_command, env=subprocess_env, check=True)
    print("Database dump completed successfully")
except subprocess.CalledProcessError as e:
    print(f"Error during pg_dump: {e}")
    exit(1)

# Check initial connection to destination Postgres
if not wait_for_postgres(host=destination_config['host']):
    exit(1)

# Load command
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql',
]

# Set environment variable for psql
subprocess_env['PGPASSWORD'] = destination_config['password']

try:
    subprocess.run(load_command, env=subprocess_env, check=True)
    print("Database load completed successfully")
except subprocess.CalledProcessError as e:
    print(f"Error during psql load: {e}")
    exit(1)

print('Ending ETL Script')
