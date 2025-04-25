import os
import shutil
import fileinput
import yaml

TEMPLATE_FILE = '/usr/local/airflow/include/template_dag.py'

def parse_yaml(filename):
    try:
        with open(filename, 'r') as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        return None

for filename in os.listdir('/usr/local/airflow/dbt/models/staging'):
    if filename == 'schema.yml':
        parsed_data = parse_yaml(f"/usr/local/airflow/dbt/models/staging/{filename}")
        if parsed_data and isinstance(parsed_data, dict) and "models" in parsed_data:
            for model in parsed_data["models"]:
                # print(model)
                new_dagfile = f"/usr/local/airflow/dags/dbt_{model['name']}.py"
                shutil.copyfile(TEMPLATE_FILE, new_dagfile)
                for line in fileinput.input(new_dagfile, inplace=True):
                    line = line.replace("MY_DAG_ID", model['name'])
                    line = line.replace("SCHEMA", model['config']['meta']['schema'])
                    line = line.replace("TABLE_NAME", model['config']['meta']['table_name'])
                    line = line.replace("MY_INTERVAL", model['config']['schedule'])
                    line = line.replace("UPDATE_TYPE", model['config']['src_cd'])
                    # line = line.replace("MY_DESCRIPTION", model['description'])
                    # line = line.replace("__doc__", model['description'])
                    line = line.replace("MY_MODEL", model['name'])
                    print(line, end="")
