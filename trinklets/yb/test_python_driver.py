import psycopg2

# Database connection parameters
DB_NAME = "yugabyte"
DB_USER = "yugabyte"
DB_PASSWORD = ""
DB_HOST = "localhost"  # Or your database host/IP
DB_PORT = "5433"       # Default PostgreSQL port

# Establish a connection to the database
conn = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
print("Database connected successfully!")

# Create a cursor object to execute SQL commands
cur = conn.cursor()
