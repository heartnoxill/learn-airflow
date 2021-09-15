try:    # everything is normal
    from datetime import timedelta
    import airflow
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd
    print("Every DAG Modules are Fine")

except Exception as e:      # in case error
    print("Error  {} ".format(e))

def first_func_test_execute(**context):
    print("first_func_test_execute")
    context['ti'].xcom_push(key='mykey', value="first_func_test_execute Hello World")

def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name":"MisterH","title":"Data Engineer"}, { "name":"MissB","title":"Data Scientist"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print(df.head())
    print('@'*66)

    print("I am in second_function_execute got value :{} from Function 1  ".format(instance))

with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=3),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    first_func_test_execute = PythonOperator(
        task_id="first_func_test_execute",
        python_callable=first_func_test_execute,
        provide_context=True,
        op_kwargs={"name":"Heart Bua"}
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True,
    )

first_func_test_execute >> second_function_execute