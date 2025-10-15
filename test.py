"""
Test script to verify database connection and data
Run this BEFORE starting your FastAPI app to diagnose issues

Usage: python test_db_connection.py
"""
import sys
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Import your settings
try:
    from app.core.config import settings
    from app.model.discovery import DiscoveryData
    from app.db.schema import get_database_url
except ImportError:
    print("Error: Cannot import app modules. Make sure you're in the project root.")
    sys.exit(1)


def test_connection():
    print("=" * 70)
    print("DATABASE CONNECTION TEST")
    print("=" * 70)
    
    # Step 1: Show configuration
    print("\n1. Configuration:")
    print(f"   POSTGRES_HOST: {settings.postgres_host}")
    print(f"   POSTGRES_PORT: {settings.postgres_port}")
    print(f"   POSTGRES_DB: {settings.postgres_db}")
    print(f"   POSTGRES_USER: {settings.postgres_user}")
    print(f"   POSTGRES_PASSWORD: {'*' * len(settings.postgres_password or '')}")
    
    # Get database URL
    try:
        db_url = get_database_url()
        # Hide password in output
        safe_url = db_url.split('@')[-1] if '@' in db_url else db_url
        print(f"\n2. Database URL: postgresql://...@{safe_url}")
    except Exception as e:
        print(f"\n2. ✗ Error getting database URL: {e}")
        return False
    
    # Step 2: Test connection
    try:
        engine = create_engine(db_url, echo=False)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"\n3. ✓ Connection successful!")
            print(f"   PostgreSQL version: {version[:50]}...")
    except Exception as e:
        print(f"\n3. ✗ Connection failed: {e}")
        print("\n   Troubleshooting:")
        print("   - Check your .env file has correct PostgreSQL credentials")
        print("   - Verify PostgreSQL is running")
        print("   - Check firewall/network settings")
        print("   - Verify database 'qupid' exists")
        return False
    
    # Step 3: Check table exists
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'discovery_data'
                )
            """))
            exists = result.fetchone()[0]
            
            if exists:
                print(f"\n4. ✓ Table 'discovery_data' exists")
            else:
                print(f"\n4. ✗ Table 'discovery_data' does NOT exist!")
                print("   You need to create this table first.")
                return False
    except Exception as e:
        print(f"\n4. ✗ Error checking table: {e}")
        return False
    
    # Step 4: Count rows
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM discovery_data"))
            count = result.fetchone()[0]
            print(f"\n5. Total rows in discovery_data: {count}")
            
            if count == 0:
                print("   ⚠ WARNING: Table is empty! No data to query.")
                return False
    except Exception as e:
        print(f"\n5. ✗ Error counting rows: {e}")
        return False
    
    # Step 5: Check table structure
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'discovery_data'
                ORDER BY ordinal_position
            """))
            columns = result.fetchall()
            print(f"\n6. Table structure:")
            for col in columns:
                print(f"   - {col[0]}: {col[1]} ({col[2]})")
    except Exception as e:
        print(f"\n6. ✗ Error getting structure: {e}")
    
    # Step 6: Sample data
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, source_id, schemas::text, timestamp 
                FROM discovery_data 
                LIMIT 3
            """))
            rows = result.fetchall()
            print(f"\n7. Sample data (first 3 rows):")
            for i, row in enumerate(rows, 1):
                print(f"\n   Row {i}:")
                print(f"      id: {row[0]}")
                print(f"      source_id: {row[1]}")
                print(f"      schemas: {row[2][:100]}..." if len(row[2]) > 100 else f"      schemas: {row[2]}")
                print(f"      timestamp: {row[3]}")
    except Exception as e:
        print(f"\n7. ✗ Error fetching sample data: {e}")
    
    # Step 7: Check unique source_ids
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT source_id 
                FROM discovery_data
                LIMIT 10
            """))
            source_ids = [row[0] for row in result.fetchall()]
            print(f"\n8. Sample source_ids (first 10):")
            for sid in source_ids:
                print(f"   - {sid}")
    except Exception as e:
        print(f"\n8. ✗ Error getting source_ids: {e}")
    
    # Step 8: Test JSONB query
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM discovery_data 
                WHERE schemas @> '["public"]'::jsonb
            """))
            count = result.fetchone()[0]
            print(f"\n9. Records with schema 'public' (JSONB query): {count}")
            
            if count == 0:
                print("   ⚠ No records found with schema='public'")
                print("   Check what values are in the 'schemas' column above")
    except Exception as e:
        print(f"\n9. ✗ JSONB query failed: {e}")
    
    # Step 9: Test specific source_id
    test_source_id = "0326af21-e5ef-45f7-98ed-a4dcbbdd1d9a"
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM discovery_data WHERE source_id = :sid"),
                {"sid": test_source_id}
            )
            count = result.fetchone()[0]
            print(f"\n10. Records with source_id '{test_source_id}': {count}")
            
            if count == 0:
                print("   ⚠ This source_id does not exist in the database")
                print("   Use one of the source_ids shown in step 8 above")
    except Exception as e:
        print(f"\n10. ✗ Source ID query failed: {e}")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
    print("\nIf all checks passed, your FastAPI app should work correctly.")
    print("If there were errors, fix the issues shown above before running the app.\n")
    
    return True


if __name__ == "__main__":
    try:
        test_connection()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)