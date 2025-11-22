"""
Comprehensive database connection test with different combinations
"""
import asyncio
import aiomysql
import ssl
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'db': os.getenv('DB_NAME'),
}

print("=" * 70)
print("COMPREHENSIVE DATABASE CONNECTION TEST")
print("=" * 70)
print(f"Host: {DB_CONFIG['host']}")
print(f"Port: {DB_CONFIG['port']}")
print(f"User: {DB_CONFIG['user']}")
print(f"Database: {DB_CONFIG['db']}")
print("=" * 70)


async def test_connection(config_override, test_name):
    """Test a connection with specific configuration"""
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print(f"{'='*70}")

    test_config = {**DB_CONFIG, **config_override}

    # Print test configuration
    for key, value in config_override.items():
        if key == 'ssl' and isinstance(value, ssl.SSLContext):
            print(f"  {key}: SSLContext(check_hostname={value.check_hostname}, verify_mode={value.verify_mode})")
        else:
            print(f"  {key}: {value}")

    conn = None
    try:
        print(f"\n⏳ Attempting connection...")
        conn = await asyncio.wait_for(
            aiomysql.connect(**test_config),
            timeout=15.0
        )

        # Test with a query
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT VERSION()")
            version = await cursor.fetchone()
            print(f"✅ SUCCESS! MySQL Version: {version[0]}")

        return True

    except asyncio.TimeoutError:
        print(f"❌ TIMEOUT after 15 seconds")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        if hasattr(e, '__class__'):
            print(f"   Error Type: {e.__class__.__name__}")
        return False
    finally:
        if conn:
            conn.close()
            await conn.wait_closed()
            print(f"🔌 Connection closed")


async def main():
    tests = []

    # Test 1: No SSL, with charset
    tests.append(({
        'ssl': None,
        'charset': 'utf8mb4',
        'connect_timeout': 10,
    }, "No SSL + UTF8MB4 Charset"))

    # Test 2: SSL disabled explicitly
    tests.append(({
        'ssl': False,
        'charset': 'utf8mb4',
        'connect_timeout': 10,
    }, "SSL=False + UTF8MB4 Charset"))

    # Test 3: SSL = True (simple SSL)
    tests.append(({
        'ssl': True,
        'charset': 'utf8mb4',
        'connect_timeout': 10,
    }, "SSL=True + UTF8MB4 Charset"))

    # Test 4: SSL Context without verification
    ssl_ctx_no_verify = ssl.create_default_context()
    ssl_ctx_no_verify.check_hostname = False
    ssl_ctx_no_verify.verify_mode = ssl.CERT_NONE
    tests.append(({
        'ssl': ssl_ctx_no_verify,
        'charset': 'utf8mb4',
        'connect_timeout': 10,
    }, "SSL Context (No Verification) + UTF8MB4"))

    # Test 5: Try without specifying database
    tests.append(({
        'ssl': None,
        'charset': 'utf8mb4',
        'connect_timeout': 10,
        'db': None,
    }, "No SSL + No Database + UTF8MB4"))

    # Test 6: SSL True without database
    tests.append(({
        'ssl': True,
        'charset': 'utf8mb4',
        'connect_timeout': 10,
        'db': None,
    }, "SSL=True + No Database + UTF8MB4"))

    # Run all tests
    for config, name in tests:
        await test_connection(config, name)
        await asyncio.sleep(1)  # Brief pause between tests

    print("\n" + "=" * 70)
    print("ALL TESTS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
