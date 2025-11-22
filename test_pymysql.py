"""
Test PyMySQL (synchronous driver) connection to DigitalOcean MySQL
"""
import pymysql
import ssl
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}

print("=" * 70)
print("PyMySQL CONNECTION TEST (Synchronous)")
print("=" * 70)
print(f"Host: {DB_CONFIG['host']}")
print(f"Port: {DB_CONFIG['port']}")
print(f"User: {DB_CONFIG['user']}")
print(f"Database: {DB_CONFIG['database']}")
print("=" * 70)


def test_connection(ssl_config, test_name):
    """Test PyMySQL connection with specific SSL configuration"""
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"{'='*70}")

    conn = None
    try:
        print(f"⏳ Attempting connection...")

        conn = pymysql.connect(
            **DB_CONFIG,
            ssl=ssl_config,
            connect_timeout=10,
            charset='utf8mb4'
        )

        # Test with a query
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"✅ SUCCESS! MySQL Version: {version[0]}")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        if hasattr(e, '__class__'):
            print(f"   Error Type: {e.__class__.__name__}")
        return False
    finally:
        if conn:
            conn.close()
            print(f"🔌 Connection closed")


# Test 1: No SSL
print("\n" + "="*70)
test_connection(None, "No SSL")

# Test 2: SSL with verification disabled
print("\n" + "="*70)
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE
test_connection({'ssl': ssl_ctx}, "SSL Context (No Verification)")

# Test 3: Empty SSL dict (forces SSL but no verification)
print("\n" + "="*70)
test_connection({}, "Empty SSL Dict")

# Test 4: SSL dict with explicit check_hostname=False
print("\n" + "="*70)
test_connection({'check_hostname': False}, "SSL Dict (check_hostname=False)")

# Test 5: Try the exact SSL config from DigitalOcean docs
print("\n" + "="*70)
test_connection({'ssl': {'ssl-mode': 'REQUIRED'}}, "DigitalOcean SSL Mode")

print("\n" + "=" * 70)
print("ALL TESTS COMPLETE")
print("=" * 70)
