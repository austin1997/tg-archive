import yaml
from pprint import pprint

# Open the YAML file for reading
with open('example_config.yaml', 'r') as file:
    # Use safe_load to parse the YAML file.
    # This converts the YAML syntax to a Python object (in this case, a dictionary)
    config = yaml.safe_load(file)

# Print the entire configuration dictionary
print("--- The full configuration dictionary ---")
pprint(config)

# You can now access the data like a normal Python dictionary
for _, group_info in config.get('groups', {}).items():
    print(f"\nGroup: {group_info.get('names')}")
    print(f"From IDs: {group_info.get('from_id')}")
    print(f"Media Directory: {group_info.get('media_dir')}")
    print(f"Download Media: {group_info.get('download_media')}")

# print("\n--- Type of the loaded object ---")
# print(type(config))
