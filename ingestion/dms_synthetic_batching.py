import pandas as pd
import numpy as np
import os
import math # Added for chunk calculations
from datetime import datetime, timedelta

# ----------------------------
# 1. Load datasets
# ----------------------------
full_load_date = datetime(2018, 8, 5, 23, 59, 59)

orders = pd.read_csv(
    "./data/raw_data/olist_orders_dataset.csv",
    parse_dates=[
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
)
order_items = pd.read_csv("./data/raw_data/olist_order_items_dataset.csv", parse_dates=['shipping_limit_date'])
customers = pd.read_csv("./data/raw_data/olist_customers_dataset.csv")
products = pd.read_csv("./data/raw_data/olist_products_dataset.csv")
sellers = pd.read_csv("./data/raw_data/olist_sellers_dataset.csv")
payments = pd.read_csv("./data/raw_data/olist_order_payments_dataset.csv")
reviews = pd.read_csv(
    "./data/raw_data/olist_order_reviews_dataset.csv",
    parse_dates=['review_creation_date', 'review_answer_timestamp']
)
category_translation = pd.read_csv("./data/raw_data/product_category_name_translation.csv")
new_row = pd.DataFrame([{'product_category_name': 'sem_categoria', 'product_category_name_english': 'uncategorized'}])
category_translation = pd.concat([category_translation, new_row], ignore_index=True)

# ----------------------------
# 3. LAD Product Isolation (Commented out in your original)
# ----------------------------

# ----------------------------
# 3. AR_H_COMMIT_TIMESTAMP
# ----------------------------  
category_translation['AR_H_COMMIT_TIMESTAMP'] = full_load_date

orders['AR_H_COMMIT_TIMESTAMP'] = np.where(
    (orders['order_status'] == 'delivered') & (orders['order_delivered_customer_date'].notna()),
    orders['order_delivered_customer_date'],
    orders['order_purchase_timestamp']
)

order_items = order_items.merge(
    orders[['order_id','order_purchase_timestamp']], on='order_id', how='left'
)
order_items['AR_H_COMMIT_TIMESTAMP'] = order_items['order_purchase_timestamp']

payments = payments.merge(
    orders[['order_id','order_purchase_timestamp']], on='order_id', how='left'
)
payments['AR_H_COMMIT_TIMESTAMP'] = payments['order_purchase_timestamp']

reviews['AR_H_COMMIT_TIMESTAMP'] = reviews['review_creation_date']

customers_commit = orders.groupby('customer_id')['AR_H_COMMIT_TIMESTAMP'].min().reset_index()
customers = customers.merge(customers_commit, on='customer_id', how='left')

products_commit = order_items.groupby('product_id')['AR_H_COMMIT_TIMESTAMP'].min().reset_index()
products = products.merge(products_commit, on='product_id', how='left')

sellers_commit = order_items.groupby('seller_id')['AR_H_COMMIT_TIMESTAMP'].min().reset_index()
sellers = sellers.merge(sellers_commit, on='seller_id', how='left')

for df in [order_items, payments]:
    df.drop(columns=['order_purchase_timestamp'], inplace=True)


# ----------------------------
# 4. Synthetic Data Creation for CDC Scenarios
# ----------------------------

# Synthetic Delete Row on Day 1 of CDC (2018-08-06)
deleted_order_id = '52ffca67c47f7bc4350f82b1d2c20dec'
deleted_row = orders[orders['order_id'] == deleted_order_id].copy()
if not deleted_row.empty:
    deleted_row['OP'] = 'D'
    deleted_row['AR_H_COMMIT_TIMESTAMP'] = datetime(2018, 8, 6, 12, 49, 18)
    orders = pd.concat([orders, deleted_row], ignore_index=True) # 73

# Synthetic Delivery Update for Order Status on Day 3 of CDC (2018-08-08)
synthetic_delivery_date = datetime(2018, 8, 8, 8, 18, 8)
synthetic_order_id = '9b745d27f038a7e9865cf448ab71e99f'
synthetic_row = orders[orders['order_id'] == synthetic_order_id].copy()
synthetic_row['order_status'] = 'delivered'
synthetic_row['order_delivered_customer_date'] = synthetic_delivery_date
synthetic_row['AR_H_COMMIT_TIMESTAMP'] = synthetic_row['order_delivered_customer_date']
synthetic_row['OP'] = 'U'
orders = pd.concat([orders, synthetic_row], ignore_index=True) # 36

# Null order_id in order_items on Day 10 of CDC (2018-08-15): Quanrantine scenario for data quality issue
quarantine_order = pd.DataFrame([{
    'order_id': None,
    'customer_id': '4e7b3e00288586ebd08712fdd0374a03',
    'order_status': 'created',
    'AR_H_COMMIT_TIMESTAMP': datetime(2018, 8, 7, 8, 0, 18),
    'OP': 'I',
}])
orders = pd.concat([orders, quarantine_order], ignore_index=True) # 18

# Negative price and null customer_id quarantine scenarios in order_items on Day 12 of CDC (2018-08-9)
quarantine_item = pd.DataFrame([{ 
    'order_id': 'c9c9e0b5e174e13426bc9daf1372bert',
    'order_item_id': 0,
    'product_id': '1e9e8ef04dbcff4541ed26657ea517e5',
    'seller_id': '3fac58ce0ad699020c7944d53c41329c',
    'price': -99.99,
    'AR_H_COMMIT_TIMESTAMP': datetime(2018, 8, 9, 9, 0, 38),
    'OP': 'I',
}])
order_items = pd.concat([order_items, quarantine_item], ignore_index=True) # 3


# LAD Scenario: order_items record appears before product ID shows up in products table (CDC Day 9 - 2018-08-14)
# Implements a planned LAD scenario where an order_item references a product_id that has not yet appeared in the products table.
# The product record will then appear later in the CDC timeline, simulating a late-arriving dimension.
lad_product_id = 'cfba8f6c12ed108481d6879597b7b521'
lad_row_captured = products[products['product_id'] == lad_product_id].copy()
# Remove it from the source so it cannot be picked up by Full Load or CDC1 loops
products = products[products['product_id'] != lad_product_id]

lad_row_captured['OP'] = 'I'
lad_row_captured['AR_H_COMMIT_TIMESTAMP'] = datetime(2018, 8, 14, 8, 0, 0)
products = pd.concat([products, lad_row_captured], ignore_index=True) # 24

# ----------------------------
# 5. Full load & CDC setup
# ----------------------------

# CHANGED: Updated to 14 days for the CDC simulation length
cdc_days = 14 

def filter_load(df, timestamp_col='AR_H_COMMIT_TIMESTAMP', start=None, end=None):
    if start is None:
        return df[df[timestamp_col] <= end].copy()
    else:
        return df[(df[timestamp_col] > start) & (df[timestamp_col] <= end)].copy()

def prepare_full_load(df):
    df_full = filter_load(df, end=full_load_date)
    df_full['OP'] = 'I'
    return df_full

orders_full = prepare_full_load(orders)
order_items_full = prepare_full_load(order_items)
payments_full = prepare_full_load(payments)
reviews_full = prepare_full_load(reviews)
customers_full = prepare_full_load(customers)
products_full = prepare_full_load(products)
sellers_full = prepare_full_load(sellers)
product_category_name_translation_full = prepare_full_load(category_translation)


# Dictionary to map table names to their respective DataFrames
source_dfs = {
    'orders': orders,
    'order_items': order_items,
    'payments': payments,
    'reviews': reviews,
    'customers': customers,
    'products': products,
    'sellers': sellers,
    'product_category_name_translation': category_translation
}


# ----------------------------
# 6. CDC Simulation (DMS Micro-batching Logic)
# ----------------------------

cdc_loads = []
# Start exactly 1 second after the full load
current_time = full_load_date + timedelta(seconds=1)

batch_size = 100
window_hours = 6
total_windows = cdc_days * (24 // window_hours)

table_names = [
    'orders', 'order_items', 'payments', 'reviews',
    'customers', 'products', 'sellers', 'product_category_name_translation'
]

# We use this to track how many rows we actually pulled vs the source
validation_counts = {t: 0 for t in table_names}

for w in range(total_windows):
    window_end = current_time + timedelta(hours=window_hours)
    
    # 1. Extract the window
    window_data = {}
    rows_in_this_window = []
    
    for table in table_names:
        # Use >= and < to ensure non-overlapping boundaries
        df_filtered = source_dfs[table][
            (source_dfs[table]['AR_H_COMMIT_TIMESTAMP'] >= current_time) & 
            (source_dfs[table]['AR_H_COMMIT_TIMESTAMP'] < window_end)
        ].copy()

        # Sort to ensure chronological chunking
        df_filtered = df_filtered.sort_values('AR_H_COMMIT_TIMESTAMP')
        
        # Apply OP flags
        if table == 'orders':
            df_filtered['OP'] = np.where(
                df_filtered['OP'] == 'D',          # preserve explicit deletes
                'D',
                np.where(
                    df_filtered['order_status'] != 'created', 'U', 'I'
                )
            )
        else:
            df_filtered['OP'] = 'I'

        window_data[table] = df_filtered
        rows_in_this_window.append(len(df_filtered))
    
    max_rows = max(rows_in_this_window)
    
    # 2. Skip if zero activity
    if max_rows == 0:
        current_time = window_end
        continue
        
    # 3. Micro-batching (The Residual Logic)
    num_chunks = math.ceil(max_rows / batch_size)
    
    for chunk_idx in range(num_chunks):
        start_row = chunk_idx * batch_size
        end_row = start_row + batch_size

        cdc_batch = {
            'window_start': current_time,
            'chunk_index': chunk_idx + 1,
            'total_chunks': num_chunks
        }
        
        for table in table_names:
            chunk = window_data[table].iloc[start_row:end_row].copy()
            cdc_batch[table] = chunk
            validation_counts[table] += len(chunk) # Log for final check
            
        cdc_loads.append(cdc_batch)
        
    current_time = window_end


# ----------------------------
# 7. Final Integrity Check
# ----------------------------

print(f"--- SIMULATION COMPLETE ---")
print(f"Total Files Generated: {len(cdc_loads)}")
print(f"Orders captured in CDC: {validation_counts['orders']}")

# Check for duplicates across all generated batches
all_order_ids = pd.concat([load['orders'] for load in cdc_loads])['order_id']
if all_order_ids.duplicated().any():
    print("❌ ERROR: Duplicate rows detected in CDC batches!")
else:
    print("✅ SUCCESS: Every row is unique and correctly windowed.")

print(f"{'Batch':<8} | {'Window Start':<15} | {'Table':<35} | {'Rows':<5}")
print("-" * 75)

last_start = None
window_counter = 0

for load in cdc_loads:
    # Increment window counter when the timestamp shifts
    if load['window_start'] != last_start:
        window_counter += 1
        last_start = load['window_start']
    
    first_table_in_chunk = True
    start_str = load['window_start'].strftime('%m-%d %H:%M')
    label = f"{window_counter}-{load['chunk_index']}"

    for table in table_names:
        row_count = len(load[table])
        
        # Only print tables that actually have data in this chunk
        if row_count > 0:
            if first_table_in_chunk:
                # Print the batch label and time for the first entry
                print(f"{label:<8} | {start_str:<15} | {table:<35} | {row_count:<5}")
                first_table_in_chunk = False
            else:
                # Indent subsequent tables in the same batch for readability
                print(f"{'':<8} | {'':<15} | {table:<35} | {row_count:<5}")
    
    # Add a small divider between different chunks/files
    print(f"{'':<8} | {'':<15} | {'-'*35} |")

print("")
print("-" * 75)
print(f"TOTAL FILES GENERATED: {len(cdc_loads)}")

# ----------------------------
# 8. Write to Disk
# ----------------------------

base_folder = "./data/dms_synthetic_data"

# 1. Write Full Load
full_load_date_str = full_load_date.strftime("%Y-%m-%d")
for table_name in table_names:
    # Build path: table/upload_date=YYYY-MM-DD/
    folder_path = os.path.join(base_folder, table_name, f"upload_date={full_load_date_str}")
    os.makedirs(folder_path, exist_ok=True)
    
    # Save the full load dataframe prepared in Section 4
    full_df = globals()[f"{table_name}_full"]
    full_df.to_csv(
        os.path.join(folder_path, "LOAD00000001.csv"), 
        index=False,
        encoding='utf-8'
    )

# 2. Write All CDC Batches (The 14-day Simulation)
for load in cdc_loads:
    # Use window_start for the partition date
    cdc_date_str = load['window_start'].strftime("%Y-%m-%d")
    # Use specific timestamp + chunk index for the filename
    timestamp_str = load['window_start'].strftime('%Y%m%d_%H%M%S')
    
    for table_name in table_names:
        df_to_save = load[table_name]
        
        # Only create a file if there is actual data for this table in this chunk
        if len(df_to_save) > 0:
            folder_path = os.path.join(base_folder, table_name, f"upload_date={cdc_date_str}")
            
            os.makedirs(folder_path, exist_ok=True)
            
            # Filename includes timestamp
            file_name = f"{timestamp_str}-{load['chunk_index']}.csv"
            df_to_save.to_csv(
                os.path.join(folder_path, file_name), 
                index=False,
                encoding='utf-8'
            )

print(f"🏁 Finalized: {len(cdc_loads)} CDC batches written to {base_folder}")