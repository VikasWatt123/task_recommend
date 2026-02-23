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


def analyze_name_email_mismatches() -> None:
    print('=== Comprehensive Analysis: SQL vs MongoDB Names and Emails ===')
    print()

    try:
        # Get MongoDB data
        db = get_db()
        mongo_employees = list(db.employee.find(
            {}, 
            {'kekaemployeenumber': 1, 'fullname': 1, 'email': 1, 'employee_name': 1, 'contact_email': 1, '_id': 0}
        ))
        
        # Create MongoDB lookup dicts
        mongo_lookup = {
            emp['kekaemployeenumber']: {
                'fullname': emp.get('fullname', ''),
                'email': emp.get('email', ''),
                'employee_name': emp.get('employee_name', ''),
                'contact_email': emp.get('contact_email', '')
            }
            for emp in mongo_employees if emp.get('kekaemployeenumber')
        }
        
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
            cursor.execute('SELECT kekaemployeenumber, fullname, email FROM up_users WHERE kekaemployeenumber IS NOT NULL AND kekaemployeenumber != "" ORDER BY kekaemployeenumber')
            mysql_employees = cursor.fetchall()
            
            print(f'  • MySQL employees: {len(mysql_employees)}')
            print()
            
            # Analysis categories
            exact_matches = []
            name_mismatches = []
            email_mismatches = []
            both_mismatches = []
            missing_data = []
            mongo_only = []
            
            for mysql_emp in mysql_employees:
                code = mysql_emp['kekaemployeenumber']
                mysql_fullname = mysql_emp['fullname'] or ''
                mysql_email = mysql_emp['email'] or ''
                
                if code in mongo_lookup:
                    mongo_data = mongo_lookup[code]
                    mongo_fullname = mongo_data['fullname'] or ''
                    mongo_email = mongo_data['email'] or ''
                    
                    # Normalize for comparison
                    mysql_name_norm = mysql_fullname.strip().lower()
                    mongo_name_norm = mongo_fullname.strip().lower()
                    mysql_email_norm = mysql_email.strip().lower()
                    mongo_email_norm = mongo_email.strip().lower()
                    
                    name_match = mysql_name_norm == mongo_name_norm
                    email_match = mysql_email_norm == mongo_email_norm
                    
                    # Calculate similarities
                    name_similarity = SequenceMatcher(None, mysql_name_norm, mongo_name_norm).ratio() if mysql_name_norm and mongo_name_norm else 0
                    email_similarity = SequenceMatcher(None, mysql_email_norm, mongo_email_norm).ratio() if mysql_email_norm and mongo_email_norm else 0
                    
                    record = {
                        'kekaemployeenumber': code,
                        'mysql_fullname': mysql_fullname,
                        'mongo_fullname': mongo_fullname,
                        'mysql_email': mysql_email,
                        'mongo_email': mongo_email,
                        'name_similarity': name_similarity,
                        'email_similarity': email_similarity,
                        'employee_name': mongo_data['employee_name'],
                        'contact_email': mongo_data['contact_email']
                    }
                    
                    if name_match and email_match:
                        exact_matches.append(record)
                    elif not name_match and not email_match:
                        both_mismatches.append(record)
                    elif not name_match:
                        name_mismatches.append(record)
                    elif not email_match:
                        email_mismatches.append(record)
                    
                    # Check for missing data
                    if not mysql_fullname.strip() or not mongo_fullname.strip() or not mysql_email.strip() or not mongo_email.strip():
                        missing_data.append(record)
                else:
                    mongo_only.append(code)
            
            # Summary statistics
            total_comparable = len(exact_matches) + len(name_mismatches) + len(email_mismatches) + len(both_mismatches)
            
            print('📊 Comparison Summary:')
            print(f'  • Exact matches (name + email): {len(exact_matches)}')
            print(f'  • Name mismatches only: {len(name_mismatches)}')
            print(f'  • Email mismatches only: {len(email_mismatches)}')
            print(f'  • Both name and email mismatches: {len(both_mismatches)}')
            print(f'  • Records with missing/empty data: {len(missing_data)}')
            print(f'  • MySQL-only records: {len(mongo_only)}')
            print(f'  • Total comparable records: {total_comparable}')
            print()
            
            # Detailed analysis for name mismatches
            if name_mismatches or both_mismatches:
                all_name_issues = name_mismatches + both_mismatches
                
                print('🔍 Name Mismatch Analysis:')
                print('  Code    | MongoDB Name                    | MySQL Name                       | Similarity | Suggestion')
                print('  -------|----------------------------------|----------------------------------|------------|-----------')
                
                # Sort by similarity (lowest first)
                all_name_issues.sort(key=lambda x: x['name_similarity'])
                
                for record in all_name_issues[:15]:  # Show first 15
                    code = record['kekaemployeenumber']
                    mongo_name = (record['mongo_fullname'] or '')[:30]
                    mysql_name = (record['mysql_fullname'] or '')[:30]
                    similarity = record['name_similarity']
                    
                    # Generate suggestion
                    suggestion = ''
                    if similarity >= 0.9:
                        suggestion = 'Minor case/whitespace fix'
                    elif similarity >= 0.7:
                        suggestion = 'Partial match - review'
                    elif similarity >= 0.5:
                        suggestion = 'Significant difference'
                    else:
                        suggestion = 'Very different - investigate'
                    
                    print(f'  {code} | {mongo_name:<30} | {mysql_name:<30} | {similarity:.2f}     | {suggestion}')
                
                if len(all_name_issues) > 15:
                    print(f'  ... and {len(all_name_issues) - 15} more name issues')
                print()
            
            # Email mismatch analysis
            if email_mismatches or both_mismatches:
                all_email_issues = email_mismatches + both_mismatches
                
                print('🔍 Email Mismatch Analysis:')
                print('  Code    | MongoDB Email                    | MySQL Email                      | Similarity | Suggestion')
                print('  -------|----------------------------------|----------------------------------|------------|-----------')
                
                # Sort by similarity (lowest first)
                all_email_issues.sort(key=lambda x: x['email_similarity'])
                
                for record in all_email_issues[:10]:  # Show first 10
                    code = record['kekaemployeenumber']
                    mongo_email = (record['mongo_email'] or '')[:30]
                    mysql_email = (record['mysql_email'] or '')[:30]
                    similarity = record['email_similarity']
                    
                    # Generate suggestion
                    suggestion = ''
                    if not mongo_email.strip() and mysql_email.strip():
                        suggestion = 'Missing in Mongo - copy from MySQL'
                    elif mongo_email.strip() and not mysql_email.strip():
                        suggestion = 'Missing in MySQL - review'
                    elif similarity >= 0.8:
                        suggestion = 'Minor typo fix'
                    else:
                        suggestion = 'Different emails - verify'
                    
                    print(f'  {code} | {mongo_email:<30} | {mysql_email:<30} | {similarity:.2f}     | {suggestion}')
                
                if len(all_email_issues) > 10:
                    print(f'  ... and {len(all_email_issues) - 10} more email issues')
                print()
            
            # Categorize issues for recommendations
            print('📋 Issue Categories & Recommendations:')
            print()
            
            # High priority name fixes (case/whitespace)
            case_name_issues = [r for r in (name_mismatches + both_mismatches) if r['name_similarity'] >= 0.9]
            if case_name_issues:
                print(f'🔧 HIGH PRIORITY - Case/Whitespace Name Fixes ({len(case_name_issues)} records):')
                for r in case_name_issues[:5]:
                    print(f'    • {r["kekaemployeenumber"]}: "{r["mongo_fullname"]}" → "{r["mysql_fullname"]}"')
                if len(case_name_issues) > 5:
                    print(f'    ... and {len(case_name_issues) - 5} more')
                print('    💡 Action: Simple trim/case correction')
                print()
            
            # Medium priority name fixes (partial matches)
            partial_name_issues = [r for r in (name_mismatches + both_mismatches) if 0.7 <= r['name_similarity'] < 0.9]
            if partial_name_issues:
                print(f'🔧 MEDIUM PRIORITY - Partial Name Matches ({len(partial_name_issues)} records):')
                for r in partial_name_issues[:3]:
                    print(f'    • {r["kekaemployeenumber"]}: "{r["mongo_fullname"]}" → "{r["mysql_fullname"]}" ({r["name_similarity"]:.2f})')
                if len(partial_name_issues) > 3:
                    print(f'    ... and {len(partial_name_issues) - 3} more')
                print('    💡 Action: Manual review and correction')
                print()
            
            # Low priority name fixes (very different)
            different_name_issues = [r for r in (name_mismatches + both_mismatches) if r['name_similarity'] < 0.7]
            if different_name_issues:
                print(f'🔧 LOW PRIORITY - Very Different Names ({len(different_name_issues)} records):')
                for r in different_name_issues[:3]:
                    print(f'    • {r["kekaemployeenumber"]}: "{r["mongo_fullname"]}" → "{r["mysql_fullname"]}" ({r["name_similarity"]:.2f})')
                if len(different_name_issues) > 3:
                    print(f'    ... and {len(different_name_issues) - 3} more')
                print('    💡 Action: Investigate data quality issues')
                print()
            
            # Email fixes
            missing_email_mongo = [r for r in (email_mismatches + both_mismatches) if not r['mongo_email'].strip() and r['mysql_email'].strip()]
            if missing_email_mongo:
                print(f'📧 EMAIL FIXES - Missing in MongoDB ({len(missing_email_mongo)} records):')
                for r in missing_email_mongo[:5]:
                    print(f'    • {r["kekaemployeenumber"]}: Add "{r["mysql_email"]}"')
                if len(missing_email_mongo) > 5:
                    print(f'    ... and {len(missing_email_mongo) - 5} more')
                print('    💡 Action: Copy from MySQL to MongoDB')
                print()
            
            # Summary recommendations
            total_fixes_needed = len(name_mismatches) + len(both_mismatches) + len(email_mismatches)
            print('📊 Summary:')
            print(f'  • Total records needing fixes: {total_fixes_needed}')
            print(f'  • High priority (easy fixes): {len(case_name_issues) + len(missing_email_mongo)}')
            print(f'  • Medium priority (review needed): {len(partial_name_issues)}')
            print(f'  • Low priority (investigate): {len(different_name_issues)}')
            print()
            print('💡 Recommended Approach:')
            print('  1. Fix case/whitespace issues first (automated)')
            print('  2. Copy missing emails from MySQL (automated)')
            print('  3. Review partial matches manually')
            print('  4. Investigate very different names last')
        
        connection.close()
        ssh_tunnel.close()

    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()

    print()
    print('✅ Analysis complete!')


if __name__ == "__main__":
    analyze_name_email_mismatches()
