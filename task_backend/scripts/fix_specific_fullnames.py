from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pymysql
import paramiko
from pymongo import UpdateOne
from sshtunnel import SSHTunnelForwarder

# Ensure the project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.settings import settings
from app.db.mongodb import get_db


def fix_specific_fullnames() -> None:
    print('=== Fixing Specific Fullnames in MongoDB ===')
    print()

    # Specific kekaemployeenumbers to fix (from user request)
    target_codes = [
        "1030", "878", "958", "30", "652", "1036", "418", "841", "576", "196",
        "767", "661", "690", "44", "172", "872", "267", "657", "641", "764",
        "768", "739", "866", "559", "763", "573", "367", "393", "610", "550",
        "786", "704", "798", "198", "1032", "1034", "843", "507", "848", "1044",
        "1043", "799", "548", "849", "900", "762", "859", "155", "938", "622",
        "277", "771", "262", "213", "717", "807", "593", "831", "692", "664",
        "783", "66", "261", "794", "445", "112", "170", "837", "85", "1016",
        "120", "373", "1018", "662", "1019", "436", "285", "1017", "12345", "0081"
    ]

    # Normalize codes (ensure they are zero-padded to 4 digits)
    normalized_codes = []
    for code in target_codes:
        # Remove leading zeros, then pad to 4 digits
        clean_code = str(code).strip().lstrip('0')
        if clean_code == '':
            clean_code = '0'
        padded_code = clean_code.zfill(4)
        normalized_codes.append(padded_code)

    print(f'📋 Target kekaemployeenumbers ({len(normalized_codes)}):')
    print(f'  • Original: {", ".join(target_codes[:10])}...')
    print(f'  • Normalized: {", ".join(normalized_codes[:10])}...')
    print()

    try:
        # Get MongoDB data for target codes (include _id for updates)
        db = get_db()
        mongo_employees = list(db.employee.find(
            {'kekaemployeenumber': {'$in': normalized_codes}},
            {'kekaemployeenumber': 1, 'fullname': 1, '_id': 1}
        ))
        
        print(f'📊 Found {len(mongo_employees)} employees in MongoDB from target list')
        
        # Create MongoDB lookup
        mongo_lookup = {emp['kekaemployeenumber']: emp['fullname'] for emp in mongo_employees}
        
        # Load SSH private key and connect to MySQL
        private_key = paramiko.RSAKey.from_private_key_file(settings.mysql_ssh_key_path)
        
        ssh_tunnel = SSHTunnelForwarder(
            (settings.mysql_ssh_host, settings.mysql_ssh_port),
            ssh_username=settings.mysql_ssh_user,
            ssh_pkey=private_key,
            remote_bind_address=(settings.mysql_host, settings.mysql_port)
        )
        ssh_tunnel.start()
        
        connection = pymysql.connect(
            host='127.0.0.1',
            port=ssh_tunnel.local_bind_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with connection.cursor() as cursor:
            # Get MySQL data for target codes
            placeholders = ','.join(['%s'] * len(normalized_codes))
            cursor.execute(
                f'SELECT kekaemployeenumber, fullname FROM up_users WHERE kekaemployeenumber IN ({placeholders}) ORDER BY kekaemployeenumber',
                normalized_codes
            )
            mysql_employees = cursor.fetchall()
            
            print(f'📊 Found {len(mysql_employees)} employees in MySQL from target list')
            print()
            
            # Compare and identify changes needed
            changes_needed = []
            not_found_in_mysql = []
            not_found_in_mongo = []
            
            for mysql_emp in mysql_employees:
                code = mysql_emp['kekaemployeenumber']
                mysql_name = mysql_emp['fullname'] or ''
                
                if code in mongo_lookup:
                    mongo_name = mongo_lookup[code] or ''
                    
                    # Compare normalized names
                    mysql_normalized = mysql_name.strip().lower()
                    mongo_normalized = mongo_name.strip().lower()
                    
                    if mysql_normalized != mongo_normalized:
                        changes_needed.append({
                            'kekaemployeenumber': code,
                            'mongo_fullname': mongo_name,
                            'mysql_fullname': mysql_name,
                            '_id': next(emp['_id'] for emp in mongo_employees if emp['kekaemployeenumber'] == code)
                        })
                else:
                    not_found_in_mongo.append(code)
            
            # Check for codes in MongoDB but not in MySQL
            for code in normalized_codes:
                if code not in [emp['kekaemployeenumber'] for emp in mysql_employees]:
                    not_found_in_mysql.append(code)
            
            print('📊 Analysis Results:')
            print(f'  • Changes needed: {len(changes_needed)}')
            print(f'  • Not found in MySQL: {len(not_found_in_mysql)}')
            print(f'  • Not found in MongoDB: {len(not_found_in_mongo)}')
            print()
            
            if changes_needed:
                print('🔍 Changes to be made:')
                print('  Code    | MongoDB Name                    | MySQL Name')
                print('  -------|----------------------------------|----------------------------------')
                
                for change in changes_needed:
                    code = change['kekaemployeenumber']
                    mongo_name = (change['mongo_fullname'] or '')[:30]
                    mysql_name = (change['mysql_fullname'] or '')[:30]
                    print(f'  {code} | {mongo_name:<30} | {mysql_name}')
                print()
                
                # Apply changes to MongoDB
                ops = []
                for change in changes_needed:
                    ops.append(UpdateOne(
                        {'_id': change['_id']},
                        {'$set': {'fullname': change['mysql_fullname']}}
                    ))
                
                if ops:
                    result = db.employee.bulk_write(ops, ordered=False)
                    print(f'✅ Applied {result.modified_count} changes to MongoDB')
                    print()
                    
                    # Verify changes
                    print('🔍 Verification:')
                    for change in changes_needed[:5]:  # Show first 5
                        code = change['kekaemployeenumber']
                        updated_doc = db.employee.find_one(
                            {'kekaemployeenumber': code},
                            {'kekaemployeenumber': 1, 'fullname': 1, '_id': 0}
                        )
                        print(f'  • {code}: "{updated_doc["fullname"]}"')
                    
                    if len(changes_needed) > 5:
                        print(f'  ... and {len(changes_needed) - 5} more updated')
            else:
                print('✅ No changes needed - all fullnames already match!')
            
            if not_found_in_mysql:
                print()
                print('⚠️  Codes not found in MySQL:')
                for code in not_found_in_mysql:
                    print(f'  • {code}')
            
            if not_found_in_mongo:
                print()
                print('⚠️  Codes not found in MongoDB:')
                for code in not_found_in_mongo:
                    print(f'  • {code}')
        
        connection.close()
        ssh_tunnel.close()

    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

    print()
    print('✅ Fullname fix complete!')


if __name__ == "__main__":
    fix_specific_fullnames()
