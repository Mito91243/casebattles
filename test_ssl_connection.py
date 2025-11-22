"""
Test different SSL connection methods for aiomysql
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

print("=" * 60)
print("SSL CONNECTION TEST")
print("=" * 60)
print(f"Host: {DB_CONFIG['host']}")
print(f"Port: {DB_CONFIG['port']}")
print(f"User: {DB_CONFIG['user']}")
print(f"Database: {DB_CONFIG['db']}")
print("=" * 60)


async def test_connection(ssl_config, test_name):
    """Test a connection with specific SSL configuration"""
    print(f"\n🔍 Testing: {test_name}")
    print(f"   SSL Config: {ssl_config}")

    conn = None
    try:
        conn = await asyncio.wait_for(
            aiomysql.connect(
                **DB_CONFIG,
                ssl=ssl_config,
                connect_timeout=10
            ),
            timeout=15.0
        )

        # Test with a query
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            result = await cursor.fetchone()

        print(f"   ✅ SUCCESS! Connected and executed query.")
        return True

    except asyncio.TimeoutError:
        print(f"   ❌ TIMEOUT after 15 seconds")
        return False
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False
    finally:
        if conn:
            conn.close()
            await conn.wait_closed()


async def main():
    # Test 1: No SSL
    await test_connection(None, "No SSL")

    # Test 2: SSL = True (simple SSL without verification)
    await test_connection(True, "SSL = True")

    # Test 3: SSL context with no verification
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    await test_connection(ssl_context, "SSL Context (no verification)")

    # Test 4: SSL context with default verification
    ssl_context_verify = ssl.create_default_context()
    await test_connection(ssl_context_verify, "SSL Context (with verification)")

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
