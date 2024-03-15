import argparse
from utils import get_delta_table


def examine_delta(table_name, metadata=False, schema=False, history=False, action=False):
    dt = get_delta_table(table_name)

    if not any([metadata, schema, history, action]):
        metadata = schema = history = action = True
        
    # Metadata
    if metadata:
        print("Metadata:")
        print(dt.metadata())

    # Schema
    if schema:
        print("Schema:")
        print(dt.schema())

    # History
    if history:
        print("History:")
        print(dt.history())

    # Get add actions
    if action:
        add_actions = dt.get_add_actions(flatten=True).to_pandas()
        print("Add Actions:")
        print(add_actions)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Examining a delta table: Metadata, Schema, History, and current add actions"
    )
    parser.add_argument("--table", "-t", required=True, help="Specify the delta table name")
    parser.add_argument("--metadata", "-m", action="store_true", help="Get metadata from the table")
    parser.add_argument("--schema", "-s", action="store_true", help="Retrieve the delta lake schema")
    parser.add_argument("--history", "-H", action="store_true", help="Get history of the delta table")
    parser.add_argument("--action", "-a", action="store_true", help="Get current add actions")

    args = parser.parse_args()

    examine_delta(args.table, args.metadata, args.schema, args.history, args.action)
