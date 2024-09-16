import subprocess
import time

def wait_for_postgres(host, max_retries = 5, delay_seconds = 5) -> None:
    '''This function will check if postgres container is active before we proceed with the ELT process'''

    retries = 0
    while retries < max_retries:

        try:
            result = subprocess.run(
                ["pg_isready", "-h", host],
                check = True,
                capture_output = True,
                text = True
            )

            if "accepting connections" in result.stdout:
                print("Connection to Postgres was successful")
                return True
        except subprocess.CalledProcessError as error:
            print("Error occurred while connecting to Postgres: ", error)
            retries += 1
            print(f"Retrying in {delay_seconds} seconds...")
            print(f"Attempt {retries}/{max_retries}")
            time.sleep(delay_seconds)
    
    # If we reach max_retries and still aren't able to connect to the database
    print("Max retries reached. Exiting...")
    return False


def main() -> None:

    # Exit if postgres cannot be found 
    if not wait_for_postgres(host = "source_postgres"):
        exit(1)
    
    # Postgres found. Start the script
    print("Initiating ELT script...")

    # Config for the source_postgres (source_db) container
    source_config: dict[str, str] = {
        'dbname':   'source_db',
        'user':     'postgres',
        'password': 'SupeRs3crE1pASS',
        'host':     'source_postgres'
    }

    # Config for the destination_postgres (destination_db) container
    destination_config: dict[str, str] = {
        'dbname':   'destination_db',
        'user':     'postgres',
        'password': 'SupeRs3crE1pASS',
        'host':     'destination_postgres'
    }

    # Use pg_dump to dump the source database to a SQL file
    # Use the -w flag to avoid prompting for password (password is set using an env variable)
    dump_command: list[str] = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w' 
]

    # Set the PGPASSWORD environment variable to avoid password prompt
    source_pgpass_env = dict(PGPASSWORD = source_config['password'])
    
    # Start the dump process
    subprocess.run(
        dump_command,
        env = source_pgpass_env,
        check = True
    )

    # Use psql to load the dumped SQL file (data_dump.sql) into the destination database
    load_command: list[str] = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

    # Set the PGPASSWORD environment variable to avoid password prompt
    destination_pgpass_env = dict(PGPASSWORD=destination_config['password'])

    # Start the load process
    subprocess.run(
        load_command,
        env = destination_pgpass_env,
        check = True
    )

if __name__ == "__main__":
    main()