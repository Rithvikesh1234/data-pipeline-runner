"""Data Pipeline Runner - Simple ETL pipeline framework."""
from typing import Callable, Any, List
import time

class Pipeline:
    def __init__(self, name: str):
        self.name = name
        self.steps: List[tuple] = []

    def pipe(self, name: str, fn: Callable):
        self.steps.append((name, fn))
        return self

    def run(self, data: Any) -> Any:
        print(f"\n🚀 Pipeline: {self.name}")
        print("=" * 40)
        for i, (name, fn) in enumerate(self.steps, 1):
            start = time.perf_counter()
            data = fn(data)
            elapsed = (time.perf_counter() - start) * 1000
            print(f"  Step {i}: {name:<25} ✅ ({elapsed:.1f}ms)")
        print("=" * 40)
        print(f"✅ Complete. Output: {data}")
        return data


# Demo: CSV-like data processing pipeline
import re

def extract(raw):
    return [line.split(",") for line in raw.strip().split("\n")]

def clean(rows):
    return [[c.strip() for c in row] for row in rows]

def filter_empty(rows):
    return [r for r in rows if all(r)]

def transform_upper(rows):
    return [[c.upper() if i == 0 else c for i, c in enumerate(r)] for r in rows]

def load(rows):
    return {"records": rows, "count": len(rows)}

if __name__ == "__main__":
    raw_data = """
    alice , 30, engineer
    bob,   25, designer
    , 40, unknown
    carol, 28, devops
    """
    Pipeline("User Data ETL") \
        .pipe("Extract rows", extract) \
        .pipe("Clean whitespace", clean) \
        .pipe("Filter empty rows", filter_empty) \
        .pipe("Uppercase names", transform_upper) \
        .pipe("Load to dict", load) \
        .run(raw_data)
