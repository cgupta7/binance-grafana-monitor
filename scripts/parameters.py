import os 
from decouple import config

# DB Details
# try:
#     postgres_config = {
#     'dbname': os.getenv('DB_NAME'),
#     'host': os.getenv('DB_HOST'),
#     'user': os.getenv('DB_USERNAME'),
#     'password': os.getenv('DB_PASSWORD'),
#     'port': int(os.getenv('DB_PORT')) if os.getenv('DB_PORT') else 5432 # type: ignore
# }
# except:
postgres_config = {
    'dbname': config('DB_NAME'),
    'host': config('DB_HOST'),
    'user': config('DB_USERNAME'),
    'password': config('DB_PASSWORD'),
    'port': int(config('DB_PORT')) if config('DB_PORT') else 5432 # type: ignore
}

if __name__ == '__main__':
    print(postgres_config)
