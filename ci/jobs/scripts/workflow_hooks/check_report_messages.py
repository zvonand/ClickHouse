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
        print(f"ERROR: {item.get('message', '')} (from: {item.get('from', '')})")
        ok = False
    for item in warnings:
        print(f"WARNING: {item.get('message', '')} (from: {item.get('from', '')})")
        ok = False

    return ok


if __name__ == "__main__":
    if not check():
        sys.exit(1)
