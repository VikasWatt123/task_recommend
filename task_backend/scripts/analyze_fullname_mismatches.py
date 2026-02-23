from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pymysql
import paramiko
from difflib import SequenceMatcher
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder

# Ensure the project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.settings import settings
from app.db.mongodb import get_db


def analyze_fullname_mismatches() -> None:
    print('=== Analyzing Fullname Mismatches: MongoDB vs MySQL ===')
    print()

    try:
        # Get MongoDB data
        db = get_db()
        mongo_employees = list(db.employee.find({}, {'kekaemployeenumber': 1, 'fullname': 1, '_id': 0}))
        
        # Create MongoDB lookup dict
        mongo_lookup = {emp['kekaemployeenumber']: emp['fullname'] for emp in mongo_employees if emp.get('kekaemployeenumber')}
        
        print(f'📊 Data Overview:')
        print(f'  • MongoDB employees: {len(mongo_employees)}')
        print()
        
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
            # Get MySQL data
            cursor.execute('SELECT kekaemployeenumber, fullname FROM up_users WHERE kekaemployeenumber IS NOT NULL AND kekaemployeenumber != "" ORDER BY kekaemployeenumber')
            mysql_employees = cursor.fetchall()
            
            print(f'  • MySQL employees: {len(mysql_employees)}')
            print()
            
            # Get all MySQL codes for lookup
            mysql_codes = {emp['kekaemployeenumber'] for emp in mysql_employees}
            
            # Compare fullnames
            matches = 0
            mismatches = []
            mongo_only = []
            mysql_only = []
            
            for mysql_emp in mysql_employees:
                mysql_code = mysql_emp['kekaemployeenumber']
                mysql_name = mysql_emp['fullname'] or ''
                
                if mysql_code in mongo_lookup:
                    mongo_name = mongo_lookup[mongo_code] or ''
                    
                    # Normalize for comparison (case-insensitive, trim whitespace)
                    mysql_normalized = mysql_name.strip().lower()
                    mongo_normalized = mongo_name.strip().lower()
                    
                    if mysql_normalized == mongo_normalized:
                        matches += 1
                    else:
                        # Calculate similarity
                        similarity = SequenceMatcher(None, mysql_normalized, mongo_normalized).ratio()
                        mismatches.append({
                            'kekaemployeenumber': mysql_code,
                            'mysql_fullname': mysql_name,
                            'mongo_fullname': mongo_name,
                            'similarity': similarity
                        })
                else:
                    mysql_only.append(mysql_code)
            
            # Check for MongoDB-only employees
            for mongo_code in mongo_lookup.keys():
                if mongo_code not in mysql_codes:
                    mongo_only.append(mongo_code)
            
            print('📊 Comparison Results:')
            print(f'  • Exact matches: {matches}')
            print(f'  • Mismatches: {len(mismatches)}')
            print(f'  • MySQL only: {len(mysql_only)}')
            print(f'  • MongoDB only: {len(mongo_only)}')
            print()
            
            # Show detailed mismatches
            if mismatches:
                print('🔍 Detailed Mismatches (MongoDB -> MySQL):')
                print('  Similarity | Code | MongoDB Name                    | MySQL Name')
                print('  ----------|------|----------------------------------|----------------------------------')
                
                # Sort by similarity (lowest first)
                mismatches.sort(key=lambda x: x['similarity'])
                
                for mismatch in mismatches[:20]:  # Show first 20
                    similarity = mismatch['similarity']
                    code = mismatch['kekaemployeenumber']
                    mongo_name = (mismatch['mongo_fullname'] or '')[:30]
                    mysql_name = (mismatch['mysql_fullname'] or '')[:30]
                    
                    print(f'  {similarity:.2f}     | {code} | {mongo_name:<30} | {mysql_name}')
                
                if len(mismatches) > 20:
                    print(f'  ... and {len(mismatches) - 20} more mismatches')
                print()
            
            # Show categories of issues
            print('📋 Issue Categories:')
            
            # Case differences
            case_issues = [m for m in mismatches if m['mongo_fullname'].strip().lower() == m['mysql_fullname'].strip().lower()]
            print(f'  • Case/whitespace differences: {len(case_issues)}')
            
            # Partial matches
            partial_matches = [m for m in mismatches if 0.5 < m['similarity'] < 1.0 and m not in case_issues]
            print(f'  • Partial matches (50-99% similar): {len(partial_matches)}')
            
            # Very different
            very_different = [m for m in mismatches if m['similarity'] <= 0.5]
            print(f'  • Very different names (≤50% similar): {len(very_different)}')
            
            # Missing values
            mongo_missing = [m for m in mismatches if not m['mongo_fullname'] or not m['mongo_fullname'].strip()]
            mysql_missing = [m for m in mismatches if not m['mysql_fullname'] or not m['mysql_fullname'].strip()]
            print(f'  • MongoDB missing/empty fullname: {len(mongo_missing)}')
            print(f'  • MySQL missing/empty fullname: {len(mysql_missing)}')
            
            print()
            
            # Show examples of each category
            if case_issues:
                print('📝 Examples of case/whitespace differences:')
                for issue in case_issues[:3]:
                    print(f'    {issue["kekaemployeenumber"]}: "{issue["mongo_fullname"]}" -> "{issue["mysql_fullname"]}"')
                print()
            
            if partial_matches:
                print('📝 Examples of partial matches:')
                for issue in partial_matches[:3]:
                    print(f'    {issue["kekaemployeenumber"]}: "{issue["mongo_fullname"]}" -> "{issue["mysql_fullname"]}" ({issue["similarity"]:.2f})')
                print()
            
            if very_different:
                print('📝 Examples of very different names:')
                for issue in very_different[:3]:
                    print(f'    {issue["kekaemployeenumber"]}: "{issue["mongo_fullname"]}" -> "{issue["mysql_fullname"]}" ({issue["similarity"]:.2f})')
                print()
            
            # Recommendations
            print('💡 Recommendations for MongoDB Corrections:')
            if case_issues:
                print(f'  • Fix case/whitespace for {len(case_issues)} records (simple trim/case correction)')
            if partial_matches:
                print(f'  • Review {len(partial_matches)} partial matches - may need manual verification')
            if very_different:
                print(f'  • Investigate {len(very_different)} very different names - possible data quality issues')
            if mongo_missing:
                print(f'  • Populate {len(mongo_missing)} empty MongoDB names from MySQL')
            
            # Show MongoDB-only employees
            if mongo_only:
                print()
                print('📋 MongoDB-only employees (not in MySQL):')
                for code in mongo_only[:10]:
                    name = mongo_lookup.get(code, 'N/A')
                    print(f'  • {code}: {name}')
                if len(mongo_only) > 10:
                    print(f'  ... and {len(mongo_only) - 10} more')
        
        connection.close()
        ssh_tunnel.close()

    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

    print()
    print('✅ Analysis complete!')


if __name__ == "__main__":
    analyze_fullname_mismatches()
