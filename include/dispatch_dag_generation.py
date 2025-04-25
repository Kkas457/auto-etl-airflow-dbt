import os
import yaml
import importlib
import sys
import glob
from pathlib import Path
from collections import defaultdict

# Configuration
DBT_PROJECT_DIR = '/usr/local/airflow/dbt/'
MODELS_DIR = os.path.join(DBT_PROJECT_DIR, 'models/staging/one_folders')
GENERATE_DAG_DIR = '/usr/local/airflow/include/'
TEMPLATE_DIR = '/usr/local/airflow/include/'
OUTPUT_DAG_DIR = '/usr/local/airflow/dags'

def find_schema_yml_files(directory):
    """Find all schema.yml files in the given directory recursively."""
    schema_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file == 'schema.yml':
                schema_files.append(os.path.join(root, file))
    return schema_files

def parse_schema_yml(schema_path):
    """Parse a schema.yml file and return a list of model metadata."""
    with open(schema_path, 'r') as f:
        try:
            schema = yaml.safe_load(f)
            if not schema or 'models' not in schema:
                print(f"Skipping {schema_path}: No models found")
                return []
            models = []
            for model in schema['models']:
                meta = model.get('config', {}).get('meta', {})
                models.append({
                    'model_name': model['name'],
                    'dag_structure_version': meta.get('dag_structure_version', 'v2'),
                    'dag_id': meta.get('dag_id', f"dbt_{model['name']}"),
                    'schema_path': schema_path,
                    'mtime': os.path.getmtime(schema_path),
                    'schema': meta.get('schema', 'unknown_schema'),
                    'table_name': meta.get('table_name', 'unknown_table'),
                    'update_type': meta.get('update_type', 'unknown_type'),
                    'owner': meta.get('owner', 'airflow'),
                    'schedule_interval': meta.get('schedule_interval', None),
                    'depends_on': model.get('depends_on', []),
                    'meta': meta  # Include meta for downstream checks
                })
            return models
        except Exception as e:
            print(f"Error parsing {schema_path}: {e}")
            return []

def aggregate_models_by_dag_id(schema_files):  # Fixed parameter name
    """Aggregate models by dag_id and ensure consistent dag_structure_version."""
    dag_models = defaultdict(list)
    for schema_file in schema_files:  # Corrected from schemaPHYSICALfiles
        models = parse_schema_yml(schema_file)
        for model in models:
            dag_id = model['dag_id']
            dag_models[dag_id].append(model)
    
    dag_info = {}
    for dag_id, models in dag_models.items():
        versions = set(model['dag_structure_version'] for model in models)
        if len(versions) > 1:
            print(f"Warning: Inconsistent dag_structure_version for dag_id '{dag_id}': {versions}. Using first version.")
        dag_info[dag_id] = {
            'models': models,
            'version': models[0]['dag_structure_version'],
            'schema_paths': [model['schema_path'] for model in models],
            'mtimes': [model['mtime'] for model in models]
        }
    return dag_info

def clean_orphaned_dags(current_dag_ids, output_dag_dir):
    existing_dags = glob.glob(os.path.join(output_dag_dir, 'dbt_*.py'))
    existing_dag_map = {os.path.basename(f).replace('.py', ''): f for f in existing_dags}
    
    print(f"Current DAG IDs: {current_dag_ids}")
    print(f"Existing DAGs: {list(existing_dag_map.keys())}")
    
    for dag_name, dag_file in existing_dag_map.items():
        # Check both the full dag_name and the name without 'dbt_' prefix
        stripped_dag_name = dag_name.replace('dbt_', '', 1)  # Strip 'dbt_' once
        if dag_name in current_dag_ids or stripped_dag_name in current_dag_ids:
            print(f"Keeping DAG '{dag_file}' (matches {dag_name} or {stripped_dag_name} in current_dag_ids)")
        else:
            print(f"Removing orphaned DAG '{dag_file}' (no corresponding schema.yml)")
            os.remove(dag_file)

def dispatch_dag_generation():
    """Dispatch DAG generation to version-specific scripts and clean up orphaned DAGs."""
    schema_files = find_schema_yml_files(MODELS_DIR)
    if not schema_files:
        print(f"No schema.yml files found in {MODELS_DIR}")
        clean_orphaned_dags(set(), OUTPUT_DAG_DIR)
        return

    dag_info = aggregate_models_by_dag_id(schema_files)  # Pass schema_files correctly
    if not dag_info:
        print("No valid models found to generate DAGs")
        clean_orphaned_dags(set(), OUTPUT_DAG_DIR)
        return

    # Generate DAGs
    for dag_id, info in dag_info.items():
        version = info['version']
        generate_module_name = f"generate_dag_{version}"
        generate_script = os.path.join(GENERATE_DAG_DIR, f"{generate_module_name}.py")

        if not os.path.exists(generate_script):
            print(f"Generate script {generate_script} not found for version '{version}', skipping {dag_id}")
            continue

        try:
            spec = importlib.util.spec_from_file_location(generate_module_name, generate_script)
            module = importlib.util.module_from_spec(spec)
            sys.modules[generate_module_name] = module
            spec.loader.exec_module(module)

            if hasattr(module, 'generate_dags'):
                module.generate_dags(
                    dag_id=dag_id,
                    models=info['models'],
                    schema_paths=info['schema_paths'],
                    output_dag_dir=OUTPUT_DAG_DIR,
                    template_dir=TEMPLATE_DIR,
                    dbt_project_dir=DBT_PROJECT_DIR
                )
            else:
                print(f"Module {generate_module_name} does not have a 'generate_dags' function")
        except Exception as e:
            print(f"Error executing {generate_script} for {dag_id}: {e}")

    # Clean up orphaned DAGs after generation
    current_dag_ids = set(dag_info.keys())
    clean_orphaned_dags(current_dag_ids, OUTPUT_DAG_DIR)

if __name__ == "__main__":
    dispatch_dag_generation()