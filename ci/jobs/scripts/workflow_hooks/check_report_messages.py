import sys

from ci.praktika.info import Info
from ci.praktika.result import Result


def check():
    info = Info()
    workflow_result = Result.from_fs(info.workflow_name)
    ext = workflow_result.ext

    errors = ext.get("errors", [])
    warnings = ext.get("warnings", [])

    ok = True
    for item in errors:
        jobs = ", ".join(item.get("jobs", []))
        print(f"ERROR: {item['message']} ({jobs})")
        ok = False
    for item in warnings:
        jobs = ", ".join(item.get("jobs", []))
        print(f"WARNING: {item['message']} ({jobs})")
        ok = False

    return ok


if __name__ == "__main__":
    if not check():
        sys.exit(1)
