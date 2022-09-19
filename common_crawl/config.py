import yaml

# Load config from YAML file
with open("config.yaml", "r") as fp:
    args = yaml.safe_load(fp)
print(args)
