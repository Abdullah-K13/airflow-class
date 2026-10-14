"""The smoke-test DAG from Part 3, step 8.

Two tasks. say_hello returns a string, say_world consumes it. They never share
memory: the value travels through the metadata database as an XCom.
"""

from airflow.sdk import dag, task
import pendulum


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
)
def hello_airflow():
    @task
    def say_hello() -> str:
        return "hello"

    @task
    def say_world(greeting: str):
        print(f"{greeting}, world")

    say_world(say_hello())


hello_airflow()
