#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from itertools import combinations
import json

import requests
from clickhouse_driver import Client
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd

DATABASE_HOST_VAR = "CHECKS_DATABASE_HOST"
DATABASE_USER_VAR = "CHECKS_DATABASE_USER"
DATABASE_PASSWORD_VAR = "CHECKS_DATABASE_PASSWORD"
S3_BUCKET = "altinity-build-artifacts"


css = """
/* Base colors inspired by Altinity */
:root {
  --altinity-blue: #007bff;
  --altinity-dark-blue: #0056b3;
  --altinity-light-gray: #f8f9fa;
  --altinity-gray: #6c757d;
  --altinity-white: #ffffff;
}

/* Body and heading fonts */
body {
  font-family: "DejaVu Sans", "Noto Sans", Arial, sans-serif;
  font-size: 0.9rem;
  background-color: var(--altinity-light-gray);
  color: var(--altinity-gray);
  padding: 2rem;
}

h1, h2, h3, h4, h5, h6 {
  color: var(--altinity-dark-blue);
}

/* General table styling */
table {
  min-width: min(900px, 98vw);
  margin: 1rem 0;
  border-collapse: collapse;
  background-color: var(--altinity-white);
  box-shadow: 0 0 8px rgba(0, 0, 0, 0.05);
}

/* Table header styling */
th {
  background-color: var(--altinity-blue);
  color: var(--altinity-white);
  padding: 10px 16px;
  text-align: left;
  border-bottom: 2px solid var(--altinity-dark-blue);
  white-space: nowrap;
}

/* Table body row styling */
tr:nth-child(even) {
  background-color: var(--altinity-light-gray);
}

tr:hover {
  background-color: var(--altinity-dark-blue);
  color: var(--altinity-white);
}

/* Table cell styling */
td {
  padding: 8px 8px;
  border-bottom: 1px solid var(--altinity-gray);
}

"""


def get_commit_statuses(sha: str) -> pd.DataFrame:
    """
    Fetch commit statuses for a given SHA and return as a pandas DataFrame.

    Args:
        sha (str): Commit SHA to fetch statuses for.

    Returns:
        pd.DataFrame: DataFrame containing all statuses.
    """
    headers = {
        "Authorization": f"token {os.getenv('GITHUB_TOKEN')}",
        "Accept": "application/vnd.github.v3+json",
    }

    url = f"https://api.github.com/repos/Altinity/ClickHouse/commits/{sha}/statuses"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch statuses: {response.status_code} {response.text}"
        )

    data = response.json()

    # Parse relevant fields
    parsed = [
        {
            "test_name": item["context"],
            "test_status": item["state"],
            # "description": item["description"],
            "results_link": item["target_url"],
        }
        for item in data
    ]

    return pd.DataFrame(parsed)


def get_checks_fails(client: Client, job_url: str):
    """
    Get tests that did not succeed for the given job URL.
    Exclude checks that have status 'error' as they are counted in get_checks_errors.
    """
    columns = (
        "check_status, check_name, test_status, test_name, report_url as results_link"
    )
    query = f"""SELECT {columns} FROM `gh-data`.checks
                WHERE task_url='{job_url}'
                AND test_status IN ('FAIL', 'ERROR')
                AND check_status!='error'
                ORDER BY check_name, test_name
                """
    return client.query_dataframe(query)


def get_checks_known_fails(client: Client, job_url: str, known_fails: dict):
    """
    Get tests that are known to fail for the given job URL.
    """
    assert len(known_fails) > 0, "cannot query the database with empty known fails"
    columns = (
        "check_status, check_name, test_status, test_name, report_url as results_link"
    )
    query = f"""SELECT {columns} FROM `gh-data`.checks
                WHERE task_url='{job_url}'
                AND test_status='BROKEN'
                AND test_name IN ({','.join(f"'{test}'" for test in known_fails.keys())})
                ORDER BY test_name, check_name
                """

    df = client.query_dataframe(query)

    df.insert(
        len(df.columns) - 1,
        "reason",
        df["test_name"]
        .cat.remove_unused_categories()
        .apply(
            lambda test_name: known_fails[test_name].get("reason", "No reason given")
        ),
    )

    return df


def get_checks_errors(client: Client, job_url: str):
    """
    Get checks that have status 'error' for the given job URL.
    """
    columns = (
        "check_status, check_name, test_status, test_name, report_url as results_link"
    )
    query = f"""SELECT {columns} FROM `gh-data`.checks
                WHERE task_url='{job_url}'
                AND check_status=='error'
                ORDER BY check_name, test_name
                """
    return client.query_dataframe(query)


def drop_prefix_rows(df, column_to_clean):
    """
    Drop rows from the dataframe if:
    - the row matches another row completely except for the specified column
    - the specified column of that row is a prefix of the same column in another row
    """
    to_drop = set()
    reference_columns = [col for col in df.columns if col != column_to_clean]
    for (i, row_1), (j, row_2) in combinations(df.iterrows(), 2):
        if all(row_1[col] == row_2[col] for col in reference_columns):
            if row_2[column_to_clean].startswith(row_1[column_to_clean]):
                to_drop.add(i)
            elif row_1[column_to_clean].startswith(row_2[column_to_clean]):
                to_drop.add(j)
    return df.drop(to_drop)


def get_regression_fails(client: Client, job_url: str):
    """
    Get regression tests that did not succeed for the given job URL.
    """
    # If you rename the alias for report_url, also update the formatters in format_results_as_html_table
    # Nested SELECT handles test reruns
    query = f"""SELECT arch, job_name, status, test_name, results_link
            FROM (
               SELECT
                    architecture as arch,
                    test_name,
                    argMax(result, start_time) AS status,
                    job_url,
                    job_name,
                    report_url as results_link
               FROM `gh-data`.clickhouse_regression_results
               GROUP BY architecture, test_name, job_url, job_name, report_url, start_time
               ORDER BY start_time DESC, length(test_name) DESC
            )
            WHERE job_url='{job_url}'
            AND status IN ('Fail', 'Error')
            """
    df = client.query_dataframe(query)
    df = drop_prefix_rows(df, "test_name")
    df["job_name"] = df["job_name"].str.title()
    return df


def url_to_html_link(url: str) -> str:
    if not url:
        return ""
    text = url.split("/")[-1]
    if not text:
        text = "results"
    return f'<a href="{url}">{text}</a>'


def format_test_name_for_linewrap(text: str) -> str:
    """Tweak the test name to improve line wrapping."""
    return text.replace(".py::", "/")


def format_test_status(text: str) -> str:
    """Format the test status for better readability."""
    color = (
        "red"
        if text.lower().startswith("fail")
        else "orange" if text.lower() == "error" else "green"
    )
    return f'<span style="font-weight: bold; color: {color}">{text}</span>'


def format_results_as_html_table(results) -> str:
    if len(results) == 0:
        return "<p>Nothing to report</p>"
    results.columns = [col.replace("_", " ").title() for col in results.columns]
    html = results.to_html(
        index=False,
        formatters={
            "Results Link": url_to_html_link,
            "Test Name": format_test_name_for_linewrap,
            "Test Status": format_test_status,
            "Check Status": format_test_status,
        },
        escape=False,
    )
    return html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a combined CI report.")
    parser.add_argument(
        "--actions-run-url", required=True, help="URL of the actions run"
    )
    parser.add_argument(
        "--pr-number", required=True, help="Pull request number for the S3 path"
    )
    parser.add_argument(
        "--commit-sha", required=True, help="Commit SHA for the S3 path"
    )
    parser.add_argument(
        "--no-upload", action="store_true", help="Do not upload the report"
    )
    parser.add_argument(
        "--known-fails", type=str, help="Path to the file with known fails"
    )
    parser.add_argument(
        "--mark-preview", action="store_true", help="Mark the report as a preview"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    db_client = Client(
        host=os.getenv(DATABASE_HOST_VAR),
        user=os.getenv(DATABASE_USER_VAR),
        password=os.getenv(DATABASE_PASSWORD_VAR),
        port=9440,
        secure="y",
        verify=False,
        settings={"use_numpy": True},
    )

    fail_results = {
        "job_statuses": get_commit_statuses(args.commit_sha),
        "checks_fails": get_checks_fails(db_client, args.actions_run_url),
        "checks_known_fails": [],
        "checks_errors": get_checks_errors(db_client, args.actions_run_url),
        "regression_fails": get_regression_fails(db_client, args.actions_run_url),
    }

    if args.known_fails:
        if not os.path.exists(args.known_fails):
            print(f"Known fails file {args.known_fails} not found.")
            exit(1)

        with open(args.known_fails) as f:
            known_fails = json.load(f)

        if known_fails:
            fail_results["checks_known_fails"] = get_checks_known_fails(
                db_client, args.actions_run_url, known_fails
            )

    title = "CI Test Report"

    html_report = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>{css}
    </style>
    <title>{title}</title>
</head>
<body>
    <h1>{title}</h1>
    <p>Generated from <a href="{args.actions_run_url}">GitHub Actions</a></p>

    <h2>Table of Contents</h2>
{'<p style="font-weight: bold;color: #F00;">This is a preview. FinishCheck has not completed.</p>' if args.mark_preview else ""}
<ul>
    <li><a href="#ci-jobs-status">CI Jobs Status</a> ({sum(fail_results['job_statuses']['test_status'] != 'success')} fail/error)</li>
    <li><a href="#checks-errors">Checks Errors</a> ({len(fail_results['checks_errors'])})</li>
    <li><a href="#checks-fails">Checks New Fails</a> ({len(fail_results['checks_fails'])})</li>
    <li><a href="#regression-fails">Regression New Fails</a> ({len(fail_results['regression_fails'])})</li>
    <li><a href="#checks-known-fails">Checks Known Fails</a> ({len(fail_results['checks_known_fails'])})</li>
</ul>

<h2 id="ci-jobs-status">CI Jobs Status</h2> 
{format_results_as_html_table(fail_results['job_statuses'])}

<h2 id="checks-errors">Checks Errors</h2>
{format_results_as_html_table(fail_results['checks_errors'])}

<h2 id="checks-fails">Checks New Fails</h2>
{format_results_as_html_table(fail_results['checks_fails'])}

<h2 id="regression-fails">Regression New Fails</h2>
{format_results_as_html_table(fail_results['regression_fails'])}

<h2 id="checks-known-fails">Checks Known Fails</h2>
{format_results_as_html_table(fail_results['checks_known_fails'])}

</body>
</html>
"""

    report_path = Path("ci_test_report.html")
    report_path.write_text(html_report, encoding="utf-8")

    if args.no_upload:
        print(f"Report saved to {report_path}")
        exit(0)

    report_destination_key = f"{args.pr_number}/{args.commit_sha}/ci_test_report.html"

    # Upload the report to S3
    s3_client = boto3.client("s3")

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=report_destination_key,
            Body=html_report,
            ContentType="text/html; charset=utf-8",
        )
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")

    print(f"https://s3.amazonaws.com/{S3_BUCKET}/" + report_destination_key)


if __name__ == "__main__":
    main()
