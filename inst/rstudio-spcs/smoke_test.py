#!/opt/conda/envs/snowflake_ml/bin/python
"""Smoke test: Snowpark + SPCS OAuth in RStudio SPCS container.

Run from RStudio Terminal:
  python3 ~/smoke_test.py
"""

from __future__ import annotations

import os
import sys


def _require(path: str) -> str:
    if not os.path.exists(path):
        print(f"FAIL: missing {path}", file=sys.stderr)
        sys.exit(1)
    return path


def main() -> None:
    print("=== Python / Snowpark smoke test ===")
    token_path = _require("/snowflake/session/token")
    host = os.environ.get("SNOWFLAKE_HOST", "")
    if not host:
        print("FAIL: SNOWFLAKE_HOST not set", file=sys.stderr)
        sys.exit(1)

    print(f"SNOWFLAKE_HOST: {host}")
    print(f"Token file: {token_path}")

    from snowflake.snowpark import Session

    token = open(token_path, encoding="utf-8").read().strip()
    session = (
        Session.builder.configs(
            {
                "host": host,
                "account": os.environ["SNOWFLAKE_ACCOUNT"],
                "authenticator": "oauth",
                "token": token,
                "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
                "database": os.environ.get("SNOWFLAKE_DATABASE", ""),
                "schema": os.environ.get("SNOWFLAKE_SCHEMA", ""),
                "role": os.environ.get("SNOWFLAKE_ROLE", ""),
            }
        ).create()
    )

    rows = session.sql(
        "SELECT CURRENT_USER() AS u, CURRENT_WAREHOUSE() AS wh, CURRENT_ROLE() AS r"
    ).collect()
    print(rows[0].as_dict() if rows else "FAIL: no rows")
    print("=== PASS ===")


if __name__ == "__main__":
    main()
