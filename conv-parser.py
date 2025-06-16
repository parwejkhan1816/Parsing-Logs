import json
from datetime import datetime, timedelta
from dateutil import parser 
from collections import defaultdict, deque
from pymongo import MongoClient
# input_file = "processed_merged_logs_april_06-29_2025.log.json"
# with open(input_file, "r") as file:
#     data = json.load(file)
# print("Data loaded from JSON file",data)

# Define output file paths
output_denied_file = "denied_logs_synopsys.json"
output_tool_list_file = "tool_list_synopsys.json"
output_out_in_file = "out_in_logs_synopsys_march_16_29_april_2025.json"
output_queued_dequeued_file = "queued_dequeued_logs_synopsys.json"
output_unsupported_file = "unsupported_logs_synopsys.json"

# OUT_IN duration calculation
def duration(in_time, out_time):
    timestamp_dt_out = datetime.strptime(out_time, "%Y-%m-%dT%H:%M:%S.%f%z")
    timestamp_dt_in = datetime.strptime(in_time, "%Y-%m-%dT%H:%M:%S.%f%z")

    epoch_time_in = int(timestamp_dt_in.timestamp())
    epoch_time_out = int(timestamp_dt_out.timestamp())

    total_seconds = epoch_time_in - epoch_time_out
    minutes = total_seconds // 60
    seconds = total_seconds % 60

    return float(f"{minutes}.{seconds:02d}")

# Function to store denied logs
def store_denied_logs():
    try: 
        denied_logs = [doc for doc in data if doc.get("status") == "DENIED"]
        with open(output_denied_file, "w") as outfile:
            json.dump(denied_logs, outfile, indent=4)
        print(f"{len(denied_logs)} denied logs written to {output_denied_file}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    

# Function to generate tool list
def tool_list():
    try:
        tool_list= []
        seen_tools = set()
        for doc in data:
            tool_key = (doc.get("tool_name"), doc.get("OEM"))
            if tool_key not in seen_tools:
                seen_tools.add(tool_key)
                tool_list.append({"tool_name": doc.get("tool_name"), "OEM": doc.get("OEM")})

        with open(output_tool_list_file, "w") as outfile:
            json.dump(tool_list, outfile, indent=4)
        print(f"{len(tool_list)} tools written to {output_tool_list_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

# Function to process OUT_IN logs

# def out_in_logs():
#     try:
#         # Step 1: Collect 'OUT' entries and store them in FIFO order
#         out_docs = []
#         out_dict = {}  # Dictionary for quick lookup

#         for doc in data:
#             if doc.get("status") == "OUT":
#                 key = (
#                     doc["tool_name"],
#                     doc["username"],
#                     doc["hostname"],
#                     doc["OEM"],
#                     doc.get("handle", None)
#                 )
#                 entry = {
#                     'out_timestamp': doc['timestamp'],
#                     'in_timestamp': None,
#                     'duration': None,
#                     'tool_name': doc['tool_name'],
#                     'username': doc['username'],
#                     'hostname': doc['hostname'],
#                     'handle': doc.get('handle', None),
#                     'OEM': doc['OEM'],
#                     'date': doc['date']
#                 }
#                 out_docs.append(entry)

#                 # Store multiple 'OUT' entries per key in FIFO order (earliest timestamps first)
#                 if key not in out_dict:
#                     out_dict[key] = []
#                 out_dict[key].append(entry)

#         # Step 2: Match 'IN' entries and update records
#         count = 0
#         for doc in data:
#             if doc.get("status") == "IN":
#                 key = (
#                     doc["tool_name"],
#                     doc["username"],
#                     doc["hostname"],
#                     doc["OEM"],
#                     doc.get("handle", None)
#                 )

#                 if key in out_dict and out_dict[key]:
#                     in_timestamp = parser.parse(doc["timestamp"])  # Parse 'IN' timestamp

#                     # Sort 'OUT' entries by timestamp to get the earliest one
#                     out_dict[key].sort(key=lambda x: parser.parse(x["out_timestamp"]))

#                     # Find the **earliest 'OUT'** document with timestamp less than 'IN'
#                     for i, out_doc in enumerate(out_dict[key]):
#                         out_timestamp = parser.parse(out_doc["out_timestamp"])  # Parse 'OUT' timestamp

#                         if out_doc["in_timestamp"] is None and in_timestamp > out_timestamp:
#                             # Update the earliest matching 'OUT' document
#                             out_doc["in_timestamp"] = doc["timestamp"]
#                             out_doc["duration"] = duration(doc["timestamp"], out_doc["out_timestamp"])
#                             count += 1
#                             print("OUT count : ",count)
#                             # Remove the matched 'OUT' log to prevent re-use
#                             del out_dict[key][i]
#                             break  # Stop after updating the **first valid 'OUT'**

#         # Save results to output JSON file
#         with open(output_out_in_file, "w") as outfile:
#             json.dump(out_docs, outfile, indent=4)

#         print(f"{count} OUT_IN logs updated and written to {output_out_in_file}")

#     except Exception as e:
#         print(f"Error in out_in_logs: {e}")

def queued_dequeued_logs():
    try:
        queued_dequeued_results = []
        queued_docs = {}

        # Step 1: Filter QUEUED documents and store them in the desired format
        for doc in data:
            if doc.get("status") == "QUEUED":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc.get("handle", "None"),  # Default to "None" if handle is missing
                    doc["OEM"]
                )
                # Store QUEUED document in the desired format
                queued_doc = {
                    "handle": doc.get("handle", "None"),
                    "queued_timestamp": doc.get("timestamp"),
                    "dequeued_timestamp": None,
                    "tool_name": doc.get("tool_name"),
                    "username": doc.get("username"),
                    "hostname": doc.get("hostname"),
                    "duration": None,
                    "OEM": doc.get("OEM"),
                    "date": doc["date"]
                }
                queued_docs[key] = queued_doc
                queued_dequeued_results.append(queued_doc)

        # Step 2: Filter DEQUEUED documents and update corresponding QUEUED documents
        for doc in data:
            if doc.get("status") == "DEQUEUED":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc.get("handle", "None"),  # Default to "None" if handle is missing
                    doc["OEM"]
                )
                # If a corresponding QUEUED document exists, update it
                if key in queued_docs:
                    queued_doc = queued_docs[key]
                    queued_doc["dequeued_timestamp"] = doc.get("timestamp")
                    queued_doc["duration"] = duration(doc.get("timestamp"),queued_doc["queued_timestamp"])

        # Save results to JSON file
        with open(output_queued_dequeued_file, "w") as outfile:
            json.dump(queued_dequeued_results, outfile, indent=4)

        print(f"{len(queued_dequeued_results)} QUEUED/DEQUEUED logs written to {output_queued_dequeued_file}")

    except Exception as e:
        print(f"Error finding documents: {e}")

# Function to store unsupported logs
def store_unsupported_logs():

    try:
            unsupported_logs = [doc for doc in data if doc.get("status") == "UNSUPPORTED"]
            with open(output_unsupported_file, "w") as outfile:
                json.dump(unsupported_logs, outfile, indent=4)
            print(f"{len(unsupported_logs)} unsupported logs written to {output_unsupported_file}")
    except Exception as e:
        print(f"some error ccured , {e}")


# Call the functions
# out_in_logs()
# queued_dequeued_logs()
# store_unsupported_logs()


def out_in_logs():
    try:
        out_dict = defaultdict(deque)
        out_docs = []

        # Step 1: Prepare OUT entries
        for doc in data:
            if doc.get("status") == "OUT":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )

                out_timestamp_obj = parser.parse(doc["timestamp"])
                entry = {
                    'out_timestamp': doc['timestamp'],
                    'out_ts_obj': out_timestamp_obj,  # for sorting and comparison
                    'in_timestamp': None,
                    'duration': None,
                    'tool_name': doc['tool_name'],
                    'username': doc['username'],
                    'hostname': doc['hostname'],
                    'handle': doc.get('handle', None),
                    'OEM': doc['OEM'],
                    'date': doc['date']
                }

                out_dict[key].append(entry)
                out_docs.append(entry)

        # Step 2: Sort each deque (once)
        for key in out_dict:
            out_dict[key] = deque(sorted(out_dict[key], key=lambda x: x['out_ts_obj']))

        # Step 3: Match IN entries to earliest valid OUT entries
        count = 0
        for doc in data:
            if doc.get("status") == "IN":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )

                in_ts = parser.parse(doc["timestamp"])
                if key in out_dict:
                    dq = out_dict[key]
                    while dq:
                        out_entry = dq[0]
                        if out_entry["in_timestamp"] is None and in_ts > out_entry["out_ts_obj"]:
                            out_entry["in_timestamp"] = doc["timestamp"]
                            out_entry["duration"] = duration(doc["timestamp"], out_entry["out_timestamp"])
                            dq.popleft()
                            count += 1
                            if count % 1000 == 0:
                                print("Matched count:", count)
                            break
                        else:
                            break

        # Cleanup: remove temporary field before saving
        for entry in out_docs:
            entry.pop('out_ts_obj', None)

        with open(output_out_in_file, "w") as outfile:
            json.dump(out_docs, outfile, indent=4)

        print(f"{count} OUT_IN logs updated and written to {output_out_in_file}")

    except Exception as e:
        print(f"Error in out_in_logs: {e}")




def build_log_files():
    global data

    if isinstance(data, dict) and "merged_mar_06_29_apr_2025" in data:
            data = data["merged_mar_06_29_apr_2025"]  # Extract the list of logs
    else:
            raise ValueError("Invalid JSON structure: Expected a dictionary with 'merged_mar_06_29_apr_2025' key.")
    
#    store_denied_logs()
#    tool_list()
#    store_unsupported_logs()
#    queued_dequeued_logs()
#    out_in_logs()

# build_log_files()


def efficient_out_in_logs(start_date=None, end_date=None):

    try:
        out_dict = defaultdict(deque)
        out_docs = []
        count = 0

        # Step 1: Get existing OUT entries from DB with null in_timestamp
        client = MongoClient("mongodb://localhost:27017/")
        db = client["eda_demo_5"]
        collection = db["out_in_log"]
        existing_outs = list(collection.find({"in_timestamp": None}))
        print(f"Found {len(existing_outs)} existing OUT entries with null in_timestamp")
        
        # Create a dictionary for existing OUTs for faster lookup
        existing_outs_dict = defaultdict(list)
        for doc in existing_outs:
            key = (
                doc["tool_name"],
                doc["username"],
                doc["hostname"],
                doc["OEM"],
                doc.get("handle", None)
            )
            existing_outs_dict[key].append({
                'out_timestamp': doc['out_timestamp'],
                'out_ts_obj': parser.parse(doc['out_timestamp']),
                '_id': doc['_id']  # Keep track of MongoDB ID for updating
            })
        
        # Sort each list by timestamp (oldest first)
        for key in existing_outs_dict:
            existing_outs_dict[key].sort(key=lambda x: x['out_ts_obj'])

        # Step 2: Prepare new OUT entries from current data
        for doc in data:
            if doc.get("status") == "OUT":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )

                out_timestamp_obj = parser.parse(doc["timestamp"])
                entry = {
                    'out_timestamp': doc['timestamp'],
                    'out_ts_obj': out_timestamp_obj,
                    'in_timestamp': None,
                    'duration': None,
                    'tool_name': doc['tool_name'],
                    'username': doc['username'],
                    'hostname': doc['hostname'],
                    'handle': doc.get('handle', None),
                    'OEM': doc['OEM'],
                    'date': doc['date']
                }

                out_dict[key].append(entry)
                out_docs.append(entry)

        # Step 3: Sort each deque (once)
        for key in out_dict:
            out_dict[key] = deque(sorted(out_dict[key], key=lambda x: x['out_ts_obj']))

        # Step 4: First process IN logs against existing database OUT entries
        for doc in data:
            if doc.get("status") == "IN":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )
                in_ts = parser.parse(doc["timestamp"])

                # First check against existing database OUTs
                if key in existing_outs_dict and existing_outs_dict[key]:
                    # Find the oldest OUT that hasn't been checked in yet
                    for i, out_entry in enumerate(existing_outs_dict[key]):
                        if in_ts > out_entry['out_ts_obj']:
                            # Update this document in MongoDB
                            update_data = {
                                'in_timestamp': doc['timestamp'],
                                'duration': duration(doc["timestamp"], out_entry['out_timestamp'])
                            }
                            collection.update_one(
                                {'_id': out_entry['_id']},
                                {'$set': update_data}
                            )
                            # Remove this entry from consideration
                            existing_outs_dict[key].pop(i)
                            count += 1
                            if count % 1000 == 0:
                                print("Matched count (existing OUTs):", count)
                            break

        # Step 5: Then process IN logs against current OUT entries as before
        for doc in data:
            if doc.get("status") == "IN":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )
                in_ts = parser.parse(doc["timestamp"])

                if key in out_dict:
                    dq = out_dict[key]
                    while dq:
                        out_entry = dq[0]
                        if out_entry["in_timestamp"] is None and in_ts > out_entry["out_ts_obj"]:
                            out_entry["in_timestamp"] = doc["timestamp"]
                            out_entry["duration"] = duration(doc["timestamp"], out_entry["out_timestamp"])
                            dq.popleft()
                            count += 1
                            if count % 1000 == 0:
                                print("Matched count (current OUTs):", count)
                            break
                        else:
                            break

        # Cleanup: remove temporary field before saving
        for entry in out_docs:
            entry.pop('out_ts_obj', None)

        with open(output_out_in_file, "w") as outfile:
            json.dump(out_docs, outfile, indent=4)

        print(f"{count} OUT_IN logs processed (existing + current). Written to {output_out_in_file}")

    except Exception as e:
        print(f"Error in out_in_logs: {e}")
    finally:
        client.close()


from collections import defaultdict, deque
from pymongo import MongoClient
import json
from dateutil import parser

def get_unmatched_checkouts(mongo_uri="mongodb://localhost:27017/", db_name="eda_demo_5", collection_name="out_in_log"):
    """
    Retrieves all documents from database where in_timestamp is None
    Returns them organized by key for efficient matching
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    query = {"in_timestamp": None}
    results = list(collection.find(query))
    print(f"Found {len(results)} unmatched checkouts in database.")
    
    # Organize by key for efficient lookup
    db_out_dict = defaultdict(deque)
    for doc in results:
        key = (
            doc["tool_name"],
            doc["username"],
            doc["hostname"],
            doc["OEM"],
            doc.get("handle", None)
        )
        # Add database ID for updating later
        doc['_db_id'] = doc['_id']
        doc['out_ts_obj'] = parser.parse(doc['out_timestamp'])
        db_out_dict[key].append(doc)
    
    # Sort each deque by checkout timestamp
    for key in db_out_dict:
        db_out_dict[key] = deque(sorted(db_out_dict[key], key=lambda x: x['out_ts_obj']))
    
    client.close()
    return db_out_dict

def update_database_record(doc_id, in_timestamp, duration, mongo_uri="mongodb://localhost:27017/", 
                          db_name="eda_demo_5", collection_name="out_in_log"):
    """
    Updates a specific document in the database with in_timestamp and duration
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    update_result = collection.update_one(
        {"_id": doc_id},
        {"$set": {"in_timestamp": in_timestamp, "duration": duration}}
    )
    client.close()
    return update_result.modified_count > 0

def out_in_logs_enhanced():
    try:
        # Get unmatched checkouts from database
        db_out_dict = get_unmatched_checkouts()
        
        out_dict = defaultdict(deque)
        out_docs = []
        matched_with_db = 0
        matched_with_current = 0

        # Step 1: Prepare OUT entries from current data
        for doc in data:
            if doc.get("status") == "OUT":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )

                out_timestamp_obj = parser.parse(doc["timestamp"])
                entry = {
                    'out_timestamp': doc['timestamp'],
                    'out_ts_obj': out_timestamp_obj,
                    'in_timestamp': None,
                    'duration': None,
                    'tool_name': doc['tool_name'],
                    'username': doc['username'],
                    'hostname': doc['hostname'],
                    'handle': doc.get('handle', None),
                    'OEM': doc['OEM'],
                    'date': doc['date']
                }

                out_dict[key].append(entry)
                out_docs.append(entry)

        # Step 2: Sort current OUT entries
        for key in out_dict:
            out_dict[key] = deque(sorted(out_dict[key], key=lambda x: x['out_ts_obj']))

        # Step 3: Process IN entries
        for doc in data:
            if doc.get("status") == "IN":
                key = (
                    doc["tool_name"],
                    doc["username"],
                    doc["hostname"],
                    doc["OEM"],
                    doc.get("handle", None)
                )

                in_ts = parser.parse(doc["timestamp"])
                matched = False

                # First, try to match with database records (unmatched checkouts)
                if key in db_out_dict:
                    db_dq = db_out_dict[key]
                    while db_dq:
                        db_entry = db_dq[0]
                        if in_ts > db_entry["out_ts_obj"]:
                            # Match found with database record
                            duration_val = duration(doc["timestamp"], db_entry["out_timestamp"])
                            
                            # Update database
                            if update_database_record(db_entry['_db_id'], doc["timestamp"], duration_val):
                                print(f"Updated database record {db_entry['_db_id']} with IN timestamp")
                                matched_with_db += 1
                                if matched_with_db % 100 == 0:
                                    print(f"Database matches: {matched_with_db}")
                            
                            db_dq.popleft()
                            matched = True
                            break
                        else:
                            break

                # If not matched with database, try current OUT entries
                if not matched and key in out_dict:
                    current_dq = out_dict[key]
                    while current_dq:
                        out_entry = current_dq[0]
                        if out_entry["in_timestamp"] is None and in_ts > out_entry["out_ts_obj"]:
                            out_entry["in_timestamp"] = doc["timestamp"]
                            out_entry["duration"] = duration(doc["timestamp"], out_entry["out_timestamp"])
                            current_dq.popleft()
                            matched_with_current += 1
                            if matched_with_current % 1000 == 0:
                                print(f"Current data matches: {matched_with_current}")
                            break
                        else:
                            break

        # Cleanup: remove temporary fields
        for entry in out_docs:
            entry.pop('out_ts_obj', None)

        # Save current processed data
        with open(output_out_in_file, "w") as outfile:
            json.dump(out_docs, outfile, indent=4)

        print(f"Processing complete:")
        print(f"  - {matched_with_db} IN logs matched with previous database checkouts")
        print(f"  - {matched_with_current} IN logs matched with current data checkouts")
        print(f"  - Results written to {output_out_in_file}")

        # Optional: Show remaining unmatched database records
        remaining_unmatched = sum(len(dq) for dq in db_out_dict.values())
        if remaining_unmatched > 0:
            print(f"  - {remaining_unmatched} database checkouts still unmatched")

    except Exception as e:
        print(f"Error in enhanced out_in_logs: {e}")

def process_logs_with_database_integration():
    """
    Main function to process logs with database integration
    """
    print("Starting enhanced OUT-IN log processing...")
    print("This will:")
    print("1. Load unmatched checkouts from database")
    print("2. Process current date range logs") 
    print("3. Match IN logs with database records first, then current data")
    print("4. Update database with matched records")
    print("-" * 50)
    
    out_in_logs_enhanced()