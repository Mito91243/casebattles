"""
Diagnostic script to verify database configuration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=" * 60)
print("DATABASE CONFIGURATION DIAGNOSTIC")
print("=" * 60)

# Check if .env file exists
if os.path.exists('.env'):
    print("✅ .env file found")
else:
    print("❌ .env file NOT found")

print("\n" + "=" * 60)
print("LOADED CONFIGURATION VALUES:")
print("=" * 60)

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_ssl_mode = os.getenv('DB_SSL_MODE')

print(f"DB_HOST     : {db_host}")
print(f"DB_PORT     : {db_port}")
print(f"DB_USER     : {db_user}")
print(f"DB_PASSWORD : {'*' * len(db_password) if db_password else 'NOT SET'}")
print(f"DB_NAME     : {db_name}")
print(f"DB_SSL_MODE : {db_ssl_mode}")

print("\n" + "=" * 60)
print("VALIDATION:")
print("=" * 60)

issues = []

if not db_host:
    issues.append("❌ DB_HOST is not set")
else:
    print(f"✅ DB_HOST is set")

if not db_port:
    issues.append("❌ DB_PORT is not set")
else:
    print(f"✅ DB_PORT is set")

if not db_user:
    issues.append("❌ DB_USER is not set")
else:
    print(f"✅ DB_USER is set")

if not db_password:
    issues.append("❌ DB_PASSWORD is not set")
else:
    print(f"✅ DB_PASSWORD is set ({len(db_password)} characters)")

if not db_name:
    issues.append("❌ DB_NAME is not set")
else:
    print(f"✅ DB_NAME is set")

print("\n" + "=" * 60)
print("MYSQL WORKBENCH COMPARISON:")
print("=" * 60)
print("Please verify these match your MySQL Workbench settings:")
print(f"  Hostname: {db_host}")
print(f"  Port: {db_port}")
print(f"  Username: {db_user}")
print(f"  Default Schema: {db_name}")
print(f"  SSL: {'Required' if db_ssl_mode == 'REQUIRED' else 'Not Required'}")

if issues:
    print("\n" + "=" * 60)
    print("ISSUES FOUND:")
    print("=" * 60)
    for issue in issues:
        print(issue)
else:
    print("\n✅ All configuration values are set!")

print("=" * 60)
