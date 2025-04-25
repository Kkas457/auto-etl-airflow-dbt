import os
import shutil
from datetime import datetime

def generate_dag_content(model):
    model_name = model['model_name']
    output_dataset = f"outlets=[Dataset('dbt://{model_name}')],"
    task_str = f"""
    run_{model_name} = BashOperator(
        task_id='run_{model_name}',
        bash_command="dbt run --select {model_name} --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        {output_dataset}
    )
"""
    return task_str

def generate_schedule_expr(models):
    # Collect external dependencies (those not in this DAG)
    model_names = set(model['model_name'] for model in models)
    external_parents = set()
    for model in models:
        for parent in model['depends_on']:
            if parent not in model_names:
                external_parents.add(parent)
    
    if not external_parents:
        # Use the first model's schedule_interval if no external dependencies
        return models[0]['schedule_interval'] or "None"
    datasets = [f"Dataset('dbt://{parent}')" for parent in external_parents]
    return " & ".join(datasets)

def generate_task_dependencies(models):
    """Generate dependency code for tasks within the DAG."""
    model_names = {model['model_name']: f"run_{model['model_name']}" for model in models}
    dependencies = []
    for model in models:
        model_task = f"run_{model['model_name']}"
        for parent in model['depends_on']:
            if parent in model_names:  # Only set dependencies for models within this DAG
                parent_task = f"run_{parent}"
                dependencies.append(f"{parent_task} >> {model_task}")
    return "\n    " + "\n    ".join(dependencies) if dependencies else ""

def generate_dags(dag_id, models, schema_paths, output_dag_dir, template_dir, dbt_project_dir):
    global DBT_PROJECT_DIR
    DBT_PROJECT_DIR = dbt_project_dir
    
    # Paths
    template_file = os.path.join(template_dir, 'template_dag_v2.py')
    dag_file = os.path.join(output_dag_dir, f"dbt_{dag_id}.py")
    max_mtime = max(model['mtime'] for model in models)
    
    # Check if DAG needs to be generated/updated
    should_generate = False
    if not os.path.exists(dag_file):
        should_generate = True
        print(f"New DAG '{dag_id}', generating")
    elif max_mtime > os.path.getmtime(dag_file):
        should_generate = True
        print(f"Schema for '{dag_id}' changed, updating DAG")
    
    if should_generate:
        if not os.path.exists(template_file):
            print(f"Template {template_file} not found, skipping")
            return
        
        with open(template_file, 'r') as f:
            template_content = f.read()
        
        now = datetime.now()
        # Generate tasks and dependencies
        task_content = "\n".join(generate_dag_content(model) for model in models)
        dependency_content = generate_task_dependencies(models)
        full_task_content = task_content + (f"\n{dependency_content}" if dependency_content else "")
        
        # Use the first model's owner
        owner = models[0]['owner']
        schedule = generate_schedule_expr(models)
        
        # Prepare documentation content
        doc_entries = []
        for model in models:
            doc_entries.append(
                f"- **{model['schema']}.{model['table_name']}** (Model: {model['model_name']}, Type: {model['update_type']})"
            )
        doc_md = "\n".join(doc_entries)
        
        dag_content = template_content.replace(
            "dag_id='dbt_template_dag'",
            f"dag_id='dbt_{dag_id}'"
        ).replace(
            '    # Placeholder for dataset tasks',
            full_task_content
        ).replace(
            "'owner': 'airflow'",
            f"'owner': '{owner}'"
        ).replace(
            "schedule=None",
            f"schedule='{schedule}'" if schedule != "None" else "schedule=None"
        ).replace(
            "DBT_PROJECT_DIR",
            DBT_PROJECT_DIR
        ).replace(
            "'start_date': 'START_TIME'",
            f"'start_date': datetime({now.year}, {now.month}, {now.day})"
        ).replace(
            'TABLES_LIST', doc_md
        ).replace(
            'MY_DAG_ID', dag_id
        )
        
        with open(dag_file, 'w') as f:
            f.write(dag_content)
        
        print(f"Generated/Updated DAG for '{dag_id}' at {dag_file}")
    else:
        print(f"DAG for '{dag_id}' unchanged, skipping")
