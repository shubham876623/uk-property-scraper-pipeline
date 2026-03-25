import csv
import random
import time
from src.handlers import process_postcode
import pandas as pd# Add once at the top of main.py
import sys
sys.stdout.reconfigure(encoding='utf-8')

def run_scraper(postcode_file, mode='sale'):
    df = pd.read_csv(postcode_file ,dtype=str)
    print(f"📋 Loaded {len(df)} postcode(s) from {postcode_file}")
    
    total_postcodes = len(df)
    successful = 0
    failed = 0
  
    for i in range(0, total_postcodes):
        postcode = df['Postcode'][i]
        outcode = df['RightmoveIdentifier'][i]
        print(f"\n{'='*80}")
        print(f"🔍 Processing postcode {i+1}/{total_postcodes}: {postcode} (outcode: {outcode})")
        print(f"{'='*80}")
        
        try:
            process_postcode(postcode, outcode)
            successful += 1
            print(f"✅ Successfully completed postcode {postcode}")
        except Exception as e:
            failed += 1
            print(f"❌ ERROR processing postcode {postcode}: {e}")
            import traceback
            traceback.print_exc()
            print(f"⚠️ Continuing to next postcode...")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SCRAPER SUMMARY")
    print(f"{'='*80}")
    print(f"Total postcodes: {total_postcodes}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"{'='*80}\n")
    
