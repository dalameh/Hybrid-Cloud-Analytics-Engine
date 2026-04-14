import boto3
import os
import json
from datetime import datetime, timedelta

# ---------------------------
# TERMINAL UI COLORS
# ---------------------------
class UI:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

# ---------------------------
# CONFIGURATION
# ---------------------------
SOURCE_DIR = "./data/dms_synthetic_data"  # local folder containing datasets
DEST_BUCKET = "olist-ecommerce-landing-zone-useast1"
LANDING_PREFIX = "landing"
REGION = "us-east-1"
EVENT_BUS_NAME = "default"  # EventBridge bus name

DATASETS = [
    "customers", "orders", "order_items", "payments",
    "products", "sellers", "reviews", "product_category_name_translation"
]

# ---------------------------
# AWS CLIENTS
# ---------------------------
s3 = boto3.client("s3", region_name=REGION)
eb = boto3.client("events", region_name=REGION)

# ---------------------------
# FUNCTIONS
# ---------------------------
def upload_file(local_path, dataset, y, m, d):
    """Uploads a single file to S3 using Hive-style partitions and sends a per-file EventBridge event."""
    filename = os.path.basename(local_path)
    s3_key = f"{LANDING_PREFIX}/{dataset}/year={y}/month={m}/day={d}/{filename}"
    try:
        s3.upload_file(local_path, DEST_BUCKET, s3_key)
        print(f"  {UI.GREEN}✔{UI.RESET} {UI.CYAN}{dataset:<12}{UI.RESET} | {filename:<25} -> {UI.GREEN}Uploaded{UI.RESET}")
        
        # Send per-file event
        detail_payload = json.dumps({"fileKey": s3_key, "status": "success"})
        eb.put_events(
            Entries=[{
                "Source": "dms.simulator",
                "DetailType": "DMSFileProcessed",
                "Detail": detail_payload,
                "EventBusName": EVENT_BUS_NAME,
                "Time": datetime.utcnow()
            }]
        )
        return True, s3_key
    except Exception as e:
        print(f"  {UI.RED}✖{UI.RESET} {UI.CYAN}{dataset:<12}{UI.RESET} | {filename:<25} -> {UI.RED}Failed: {e}{UI.RESET}")
        return False, local_path

def simulate_dms(upload_date, is_full_load=False):
    """Simulates DMS full/CDC load for a given date."""
    
    # Parse date components for the partition structure
    dt = datetime.strptime(upload_date, "%Y-%m-%d")
    y, m, d = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
    
    all_uploaded = []
    failed_files = []
    
    load_type = "FULL LOAD" if is_full_load else "CDC INCREMENT"
    print(f"\n{UI.BOLD}{UI.MAGENTA}[AWS DMS SIMULATOR] Executing {load_type} for Date: {upload_date}{UI.RESET}")
    print("-" * 70)

    found_data = False

    for dataset in DATASETS:
        # Build local hierarchical path
        dataset_path = os.path.join(SOURCE_DIR, dataset, f"year={y}", f"month={m}", f"day={d}")
        
        if not os.path.exists(dataset_path):
            continue
            
        files = [f for f in os.listdir(dataset_path) if f.endswith(".parquet")] # csv or parquet
        if files:
            found_data = True
            for file in files:
                full_path = os.path.join(dataset_path, file)
                success, result = upload_file(full_path, dataset, y, m, d)
                if success:
                    all_uploaded.append(result)
                else:
                    failed_files.append(result)

    if not found_data:
        print(f"  {UI.YELLOW}⚠ No transaction logs found for {upload_date} at year={y}/month={m}/day={d}. Skipping...{UI.RESET}")
        return

    # Send final task completed event (Safely format JSON)
    status = "success" if not failed_files else "failed"
    task_payload = json.dumps({
        "status": status,
        "filesProcessed": all_uploaded,
        "failedFiles": failed_files
    })
    
    eb.put_events(
        Entries=[{
            "Source": "dms.simulator",
            "DetailType": "DMSTaskCompleted",
            "Detail": task_payload,
            "EventBusName": EVENT_BUS_NAME,
            "Time": datetime.utcnow()
        }]
    )
    
    print("-" * 70)
    print(f"{UI.BOLD}BATCH SUMMARY FOR {upload_date}:{UI.RESET}")
    print(f"Total Files Uploaded : {UI.GREEN}{len(all_uploaded)}{UI.RESET}")
    if failed_files:
        print(f"Total Files Failed   : {UI.RED}{len(failed_files)}{UI.RESET}")
    print(f"{UI.MAGENTA}Task Event Published to EventBridge: [{EVENT_BUS_NAME}]{UI.RESET}\n")

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    print(f"\n{UI.BOLD}{UI.CYAN}🚀 INITIALIZING AWS DMS Full Load + CDC (Ongoing Replication) PIPELINE SIMULATOR OF ON-PREMISE RELATIONAL DATABASE{UI.RESET}")
    print(f"Target Bucket : {DEST_BUCKET}")
    print(f"Target Region : {REGION}")
    print("=" * 70)

    # 1. Full Load Phase
    full_load_date_str = "2018-08-05"
    print(f"\n{UI.BOLD}[PHASE 1] Initial Snapshot (Full Load){UI.RESET}")
    input(f"{UI.YELLOW}Press [ENTER] to trigger the Full Load for {full_load_date_str}...{UI.RESET}")
    simulate_dms(full_load_date_str, is_full_load=True)

    # 2. Transition to CDC Phase
    print(f"\n{UI.BOLD}{UI.GREEN}✓ Full Load Complete.{UI.RESET} {UI.BOLD}[PHASE 2] Transitioning to Ongoing Replication (CDC)...{UI.RESET}")
    
    start_cdc_date = datetime(2018, 8, 6)
    end_cdc_date = datetime(2018, 8, 19)
    current_date = start_cdc_date

    while current_date <= end_cdc_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        user_input = input(f"{UI.YELLOW}Press [ENTER] to simulate the stream of CDC row-level changes (I/U/D) for {date_str} (or 'q' to quit): {UI.RESET}")       
         
        if user_input.strip().lower() == 'q':
            print(f"\n{UI.RED}Simulation aborted by user.{UI.RESET}")
            break
            
        simulate_dms(date_str, is_full_load=False)
        current_date += timedelta(days=1)

    print(f"{UI.BOLD}{UI.CYAN}🏁 SIMULATION COMPLETE. All micro-batches have landed in S3.{UI.RESET}\n")