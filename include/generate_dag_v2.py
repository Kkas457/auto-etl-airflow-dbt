import os
import shutil
from datetime import datetime
import fileinput
import glob

def generate_dag_content(model_name):
    output_dataset = f"outlets=[Dataset('dbt://{model_name}')],"
    task_str = f"""
    run_model = BashOperator(
        task_id='run_{model_name}',
        bash_command="dbt run --select {model_name} --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        {output_dataset}
    )
"""
    return task_str

def generate_schedule_expr(parents):
    if not parents:
        return "None"
    
    # dataset_expr = []
    # for i, parent in enumerate(parents):
    #     dataset = f"Dataset('dbt://{parent}')"
    #     if i == 0:
    #         dataset_expr.append(dataset)
    #     elif i == 1:
    #         dataset_expr.append(f"({dataset}")
    #     else:
    #         dataset_expr[-1] += f" & {dataset}"
    
    # if len(parents) > 1:
    #     dataset_expr[-1] += ")"
    
    # if len(parents) > 2:
    #     return "(" + " & ".join(dataset_expr) + ")"
    # return " & ".join(dataset_expr)
    
    dataset_expr = [f"Dataset('dbt://{parent}')" for parent in parents]
    return "[" + ", ".join(dataset_expr) + "]"


def generate_dags(dag_id, models, schema_paths, output_dag_dir, template_dir, dbt_project_dir):
    global DBT_PROJECT_DIR
    DBT_PROJECT_DIR = dbt_project_dir
    
    if len(models) > 1:
        print(f"Warning: generate_dag_v1 supports only one model per DAG. Using first model for dag_id '{dag_id}'.")
    model = models[0]
    model_name = model['model_name']
    
    if 'dag_id' not in model.get('meta', {}) and dag_id == f"dbt_{model_name}":
        print(f"Warning: No 'dag_id' specified in {model['schema_path']}. Defaulting to 'dbt_{model_name}'.")
    
    # Paths (fix: don’t add extra 'dbt_' prefix since dag_id already has it)
    template_file = os.path.join(template_dir, 'template_dag_v2.py')
    dag_file = os.path.join(output_dag_dir, f"{dag_id}.py")  # Changed from f"dbt_{dag_id}.py"
    max_mtime = max(model['mtime'] for model in models)
    
    should_generate = False
    existing_dags = glob.glob(os.path.join(output_dag_dir, 'dbt_*.py'))
    existing_dag_map = {os.path.basename(f).replace('.py', '').replace('dbt_', '', 1): f  # Strip 'dbt_' once
                       for f in existing_dags}
    
    if dag_id not in existing_dag_map:
        should_generate = True
        print(f"New model '{dag_id}', generating DAG")
    else:
        dag_mtime = os.path.getmtime(existing_dag_map[dag_id])
        if max_mtime > dag_mtime:
            should_generate = True
            print(f"Schema for '{dag_id}' changed, updating DAG")
    
    if should_generate:
        if not os.path.exists(template_file):
            print(f"Template {template_file} not found, skipping")
            return
        
        with open(template_file, 'r') as f:
            template_content = f.read()
        
        now = datetime.now()
        schedule_expr = generate_schedule_expr(model['depends_on'])
        
        dag_content = template_content.replace(
            "dag_id='dbt_template_dag'",
            f"dag_id='{dag_id}'"  # Use dag_id as-is
        ).replace(
            '    # Placeholder for dataset tasks',
            generate_dag_content(model_name)
        ).replace(
            "'owner': 'airflow'",
            f"'owner': '{model['owner']}'"
        ).replace(
            "schedule_interval=None",
            f"schedule={schedule_expr}"
        ).replace(
            "DBT_PROJECT_DIR",
            DBT_PROJECT_DIR
        ).replace(
            "'start_date': 'START_TIME'",
            f"'start_date': datetime({now.year}, {now.month}, {now.day})"
        )
        
        with open(dag_file, 'w') as f:
            f.write(dag_content)
        
        for line in fileinput.input(dag_file, inplace=True):
            line = line.replace('MY_MODEL', model_name)
            line = line.replace('SCHEMA', model['schema'])
            line = line.replace('TABLE_NAME', model['table_name'])
            line = line.replace('UPDATE_TYPE', model['update_type'])
            print(line, end='')
        
        print(f"Generated/Updated DAG for '{dag_id}' at {dag_file}")
    else:
        print(f"DAG for '{dag_id}' unchanged, skipping")