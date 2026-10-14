"""Arithmetic demo DAG — the one to break on purpose.

Five tasks: a fan-out (add and multiply run in parallel off the same list), a
fan-in (summarize waits for both), and one classic operator at the end. All of
it is arithmetic, so a changed number shows up in the logs immediately.

Things worth changing in front of a class are listed in README.md.
"""

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

START = 1
COUNT = 5


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "teaching"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(seconds=10)},
)
def simple_math():
    @task
    def make_numbers() -> list[int]:
        numbers = list(range(START, START + COUNT))
        print(f"made {numbers}")
        return numbers

    @task
    def add_them(numbers: list[int]) -> int:
        total = sum(numbers)
        print(f"sum{tuple(numbers)} = {total}")
        return total

    @task
    def multiply_them(numbers: list[int]) -> int:
        product = 1
        for n in numbers:
            product *= n
        print(f"product{tuple(numbers)} = {product}")
        return product

    @task
    def summarize(total: int, product: int) -> str:
        line = f"sum={total} product={product} ratio={product / total:.2f}"
        print(line)
        return line

    announce = BashOperator(
        task_id="announce",
        bash_command="echo 'ANSWER: {{ ti.xcom_pull(task_ids=\"summarize\") }}'",
    )

    numbers = make_numbers()
    summarize(add_them(numbers), multiply_them(numbers)) >> announce


simple_math()
