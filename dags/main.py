from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.bases.operator import chain

@dag(schedule=None, description="test")
def my_dag():

  @task
  def task_a():
    print("a")

  @task
  def task_b():
    print("a")

  @task
  def task_c():
    print("a")

  @task
  def task_d():
    print("a")

  chain(task_a(), [task_b(), task_c()], task_d())

my_dag()
