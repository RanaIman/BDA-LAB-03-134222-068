import pandas as pd
import os
import argparse

DATA_FILE = "paraquat_data.parquet"

if not os.path.exists(DATA_FILE):
    data = pd.DataFrame(columns=["id", "chemical_name", "concentration", "location", "date"])
    data.to_parquet(DATA_FILE, index=False)

def load_data():
    return pd.read_parquet(DATA_FILE)

def save_data(data):
    data.to_parquet(DATA_FILE, index=False)

def create_entry(entry):
    data = load_data()
    entry['id'] = data['id'].max() + 1 if not data.empty else 1
    data = pd.concat([data, pd.DataFrame([entry])], ignore_index=True)
    save_data(data)

def read_entries():
    data = load_data()
    print(data.to_string(index=False))

def update_entry(entry_id, field, value):
    data = load_data()
    if entry_id in data['id'].values and field in data.columns:
        data.loc[data['id'] == entry_id, field] = value
        save_data(data)

def delete_entry(entry_id):
    data = load_data()
    data = data[data['id'] != entry_id]
    save_data(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["create", "read", "update", "delete"])
    parser.add_argument("--id", type=int)
    parser.add_argument("--field")
    parser.add_argument("--value")
    parser.add_argument("--chemical_name")
    parser.add_argument("--concentration")
    parser.add_argument("--location")
    parser.add_argument("--date")
    args = parser.parse_args()

    if args.action == "create":
        entry = {
            "chemical_name": args.chemical_name,
            "concentration": args.concentration,
            "location": args.location,
            "date": args.date
        }
        create_entry(entry)
    elif args.action == "read":
        read_entries()
    elif args.action == "update":
        update_entry(args.id, args.field, args.value)
    elif args.action == "delete":
        delete_entry(args.id)
