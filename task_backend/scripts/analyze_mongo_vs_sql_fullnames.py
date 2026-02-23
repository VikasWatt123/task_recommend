from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pymysql
import paramiko
from difflib import SequenceMatcher
from sshtunnel import SSHTunnelForwarder

# Ensure the project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.settings import settings
from app.db.mongodb import get_db


def analyze_mongo_vs_sql_fullnames() -> None:
    print('=== MongoDB vs SQL Fullname Analysis (MongoDB Scope Only) ===')
    print()

    try:
        # Get MongoDB data - only kekaemployeenumbers that exist in MongoDB
        db = get_db()
        mongo_employees = list(db.employee.find(
            {}, 
            {'kekaemployeenumber': 1, 'fullname': 1, '_id': 0}
        ))
        
        # Create list of MongoDB kekaemployeenumbers
        mongo_codes = [emp['kekaemployeenumber'] for emp in mongo_employees if emp.get('kekaemployeenumber')]
        mongo_lookup = {emp['kekaemployeenumber']: emp.get('fullname', '') for emp in mongo_employees if emp.get('kekaemployeenumber')}
        
        print(f'📊 MongoDB Scope:')
        print(f'  • MongoDB employees: {len(mongo_employees)}')
        print(f'  • MongoDB kekaemployeenumbers: {len(mongo_codes)}')
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
            # Get MySQL data ONLY for kekaemployeenumbers that exist in MongoDB
            placeholders = ','.join(['%s'] * len(mongo_codes))
            cursor.execute(
                f'SELECT kekaemployeenumber, fullname FROM up_users WHERE kekaemployeenumber IN ({placeholders}) ORDER BY kekaemployeenumber',
                mongo_codes
            )
            mysql_employees = cursor.fetchall()
            
            print(f'📊 MySQL Comparison:')
            print(f'  • MySQL records matching MongoDB codes: {len(mysql_employees)}')
            print()
            
            # Analysis
            exact_matches = []
            mismatches = []
            mysql_missing_for_mongo = []
            
            for mongo_code in mongo_codes:
                mongo_fullname = mongo_lookup[mongo_code] or ''
                
                # Find matching MySQL record
                mysql_record = next((emp for emp in mysql_employees if emp['kekaemployeenumber'] == mongo_code), None)
                
                if mysql_record:
                    mysql_fullname = mysql_record['fullname'] or ''
                    
                    # Normalize for comparison
                    mongo_normalized = mongo_fullname.strip().lower()
                    mysql_normalized = mysql_fullname.strip().lower()
                    
                    if mongo_normalized == mysql_normalized:
                        exact_matches.append({
                            'kekaemployeenumber': mongo_code,
                            'mongo_fullname': mongo_fullname,
                            'mysql_fullname': mysql_fullname
                        })
                    else:
                        # Calculate similarity
                        similarity = SequenceMatcher(None, mongo_normalized, mysql_normalized).ratio()
                        
                        mismatches.append({
                            'kekaemployeenumber': mongo_code,
                            'mongo_fullname': mongo_fullname,
                            'mysql_fullname': mysql_fullname,
                            'similarity': similarity
                        })
                else:
                    mysql_missing_for_mongo.append({
                        'kekaemployeenumber': mongo_code,
                        'mongo_fullname': mongo_fullname
                    })
            
            # Summary
            total_mongo = len(mongo_codes)
            print('📊 Analysis Summary:')
            print(f'  • Total MongoDB employees: {total_mongo}')
            print(f'  • Exact matches: {len(exact_matches)}')
            print(f'  • Mismatches: {len(mismatches)}')
            print(f'  • Not found in MySQL: {len(mysql_missing_for_mongo)}')
            print(f'  • Match rate: {len(exact_matches)/total_mongo*100:.1f}%')
            print()
            
            # Detailed mismatches
            if mismatches:
                print('🔍 Detailed Mismatches (MongoDB → SQL):')
                print('  Code    | MongoDB Fullname                | SQL Fullname                     | Similarity | Change Needed')
                print('  -------|----------------------------------|----------------------------------|------------|--------------')
                
                # Sort by similarity (lowest first)
                mismatches.sort(key=lambda x: x['similarity'])
                
                for mismatch in mismatches:
                    code = mismatch['kekaemployeenumber']
                    mongo_name = mismatch['mongo_fullname'][:30]
                    sql_name = mismatch['mysql_fullname'][:30]
                    similarity = mismatch['similarity']
                    
                    # Change needed description
                    if similarity >= 0.9:
                        change_needed = 'Case/whitespace fix'
                    elif similarity >= 0.7:
                        change_needed = 'Partial match - review'
                    elif similarity >= 0.5:
                        change_needed = 'Significant difference'
                    else:
                        change_needed = 'Very different'
                    
                    print(f'  {code} | {mongo_name:<30} | {sql_name:<30} | {similarity:.2f}     | {change_needed}')
                print()
                
                # Categorize by priority
                print('📋 Change Categories:')
                
                # High priority (case/whitespace)
                high_priority = [m for m in mismatches if m['similarity'] >= 0.9]
                medium_priority = [m for m in mismatches if 0.7 <= m['similarity'] < 0.9]
                low_priority = [m for m in mismatches if m['similarity'] < 0.7]
                
                print(f'  🔧 HIGH PRIORITY (Case/whitespace fixes): {len(high_priority)} records')
                if high_priority:
                    for m in high_priority[:5]:
                        print(f'     • {m["kekaemployeenumber"]}: "{m["mongo_fullname"]}" → "{m["mysql_fullname"]}"')
                    if len(high_priority) > 5:
                        print(f'     ... and {len(high_priority) - 5} more')
                print()
                
                print(f'  🔧 MEDIUM PRIORITY (Partial matches): {len(medium_priority)} records')
                if medium_priority:
                    for m in medium_priority[:3]:
                        print(f'     • {m["kekaemployeenumber"]}: "{m["mongo_fullname"]}" → "{m["mysql_fullname"]}" ({m["similarity"]:.2f})')
                    if len(medium_priority) > 3:
                        print(f'     ... and {len(medium_priority) - 3} more')
                print()
                
                print(f'  🔧 LOW PRIORITY (Very different): {len(low_priority)} records')
                if low_priority:
                    for m in low_priority[:3]:
                        print(f'     • {m["kekaemployeenumber"]}: "{m["mongo_fullname"]}" → "{m["mysql_fullname"]}" ({m["similarity"]:.2f})')
                    if len(low_priority) > 3:
                        print(f'     ... and {len(low_priority) - 3} more')
                print()
            
            # Records not found in MySQL
            if mysql_missing_for_mongo:
                print('⚠️  MongoDB Records Not Found in MySQL:')
                for record in mysql_missing_for_mongo:
                    code = record['kekaemployeenumber']
                    name = record['mongo_fullname']
                    print(f'  • {code}: "{name}"')
                print()
            
            # Exact matches sample
            if exact_matches:
                print('✅ Sample of Exact Matches (No changes needed):')
                for match in exact_matches[:5]:
                    code = match['kekaemployeenumber']
                    name = match['mongo_fullname']
                    print(f'  • {code}: "{name}"')
                if len(exact_matches) > 5:
                    print(f'  ... and {len(exact_matches) - 5} more exact matches')
                print()
            
            # Final recommendations
            print('💡 Recommendations:')
            total_changes_needed = len(mismatches)
            if total_changes_needed == 0:
                print('  🎉 All MongoDB fullnames already match SQL! No changes needed.')
            else:
                print(f'  📝 Total changes needed: {total_changes_needed} out of {total_mongo} MongoDB records')
                print(f'  📊 Change breakdown:')
                if high_priority:
                    print(f'     • High priority (easy fixes): {len(high_priority)}')
                if medium_priority:
                    print(f'     • Medium priority (review needed): {len(medium_priority)}')
                if low_priority:
                    print(f'     • Low priority (investigate): {len(low_priority)}')
                print()
                print('  🚀 Suggested approach:')
                print('     1. Apply high priority fixes first (automated case/whitespace)')
                print('     2. Review medium priority changes manually')
                print('     3. Investigate low priority differences last')
        
        connection.close()
        ssh_tunnel.close()

    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

    print()
    print('✅ Analysis complete!')


if __name__ == "__main__":
    analyze_mongo_vs_sql_fullnames()
