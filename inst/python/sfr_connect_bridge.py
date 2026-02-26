"""
Snowflake Connection Bridge
============================

Python backend for snowflakeR::R/connect.R.

Handles Snowpark session creation, active session detection
for Workspace Notebooks, and robust pandas → R data conversion.
"""


import os
from typing import Optional


def get_active_session():
    """
    Get the active Snowpark session (works in Workspace Notebooks).

    Returns:
        Snowpark Session object

    Raises:
        Exception if no active session is available
    """
    from snowflake.snowpark.context import get_active_session
    return get_active_session()


def _load_private_key(key_path: str) -> bytes:
    """
    Load a PEM private key file and return the DER-encoded bytes.

    Snowpark Session.builder requires the private key as DER bytes
    in the 'private_key' parameter.

    Args:
        key_path: Path to PEM-encoded private key (.p8 file)

    Returns:
        DER-encoded private key bytes
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    key_path = os.path.expanduser(key_path)
    with open(key_path, "rb") as f:
        key_data = f.read()

    private_key = serialization.load_pem_private_key(
        key_data,
        password=None,
        backend=default_backend(),
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def create_session(
    account: str,
    user: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    role: Optional[str] = None,
    authenticator: Optional[str] = None,
    private_key_file: Optional[str] = None,
    **kwargs,
):
    """
    Create a new Snowpark session from explicit parameters.

    Supports multiple auth methods:
    - Key-pair (SNOWFLAKE_JWT): pass private_key_file path
    - External browser: authenticator="externalbrowser"
    - Username/password: authenticator="snowflake" + password in kwargs

    Args:
        account: Snowflake account identifier
        user: Username
        warehouse: Default warehouse
        database: Default database
        schema: Default schema
        role: Role to use
        authenticator: Authentication method
        private_key_file: Path to PEM private key (.p8 file)
        **kwargs: Additional connection parameters

    Returns:
        Snowpark Session object
    """
    from snowflake.snowpark import Session

    conn_params = {"account": account}

    if user:
        conn_params["user"] = user
    if warehouse:
        conn_params["warehouse"] = warehouse
    if database:
        conn_params["database"] = database
    if schema:
        conn_params["schema"] = schema
    if role:
        conn_params["role"] = role

    # Handle key-pair authentication
    if private_key_file:
        conn_params["private_key"] = _load_private_key(private_key_file)
        # Don't pass authenticator for key-pair; Snowpark auto-detects
    elif authenticator:
        conn_params["authenticator"] = authenticator

    conn_params.update(kwargs)

    session = Session.builder.configs(conn_params).create()
    return session


def pandas_to_r_dict(pdf):
    """
    Convert a pandas DataFrame to a column-oriented Python dict
    with native Python types (no numpy arrays).

    Uses Series.tolist() which converts numpy scalars to native
    Python types (int, float, str) inside NumPy's own C code,
    avoiding the NumPy 1.x/2.x ABI mismatch that occurs when
    reticulate tries to read numpy arrays directly.

    Args:
        pdf: A pandas DataFrame.

    Returns:
        dict[str, list]: Column name → list of native Python values.
    """
    cols = list(pdf.columns)
    return {col: pdf[col].tolist() for col in cols}


def generate_jwt_token(session) -> str:
    """Generate a JWT token from the session's key-pair credentials.

    Extracts the private key from the Snowpark session's connection
    and generates a short-lived JWT suitable for SPCS REST endpoint
    authentication.

    Args:
        session: Snowpark Session object (must use key-pair auth).

    Returns:
        JWT token string.

    Raises:
        RuntimeError: if the session doesn't use key-pair auth or
            the token cannot be generated.
    """
    import hashlib
    import base64
    import time

    try:
        import jwt as pyjwt
    except ImportError:
        raise RuntimeError(
            "PyJWT package is required for JWT generation. "
            "Install with: pip install PyJWT"
        )
    from cryptography.hazmat.primitives import serialization

    # Navigate to the underlying connector to extract credentials.
    # The Snowpark connector stores the private key as DER bytes
    # on conn_obj._private_key after key-pair authentication.
    try:
        conn_obj = session._conn._conn
        pk_data = getattr(conn_obj, "_private_key", None)
    except AttributeError:
        pk_data = None

    if pk_data is None:
        raise RuntimeError(
            "Cannot extract private key from session. "
            "This connection may not use key-pair authentication."
        )

    # pk_data is DER-encoded bytes -- load into a cryptography key object
    from cryptography.hazmat.backends import default_backend

    if isinstance(pk_data, bytes):
        pk = serialization.load_der_private_key(
            pk_data, password=None, backend=default_backend()
        )
    else:
        raise RuntimeError(
            f"Unexpected private key type: {type(pk_data)}. "
            "Expected DER-encoded bytes."
        )

    pub = pk.public_key()

    pub_der = pub.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    fp = base64.standard_b64encode(
        hashlib.sha256(pub_der).digest()
    ).decode("utf-8")

    # Get account and user from the session
    account = (
        session.get_current_account()
        .strip('"')
        .upper()
        .replace(".", "-")
    )
    user = session.get_current_user().strip('"').upper()

    now = int(time.time())
    payload = {
        "iss": f"{account}.{user}.SHA256:{fp}",
        "sub": f"{account}.{user}",
        "iat": now,
        "exp": now + 3600,
    }

    return pyjwt.encode(payload, pk, algorithm="RS256")


def query_to_dict(session, sql):
    """
    Execute a SQL query and return the result as a column-oriented dict
    with native Python types only (safe for reticulate conversion to R).

    This keeps the entire pandas/numpy interaction on the Python side,
    avoiding reticulate's NumPy version checks which can fail with
    NumPy 1.x/2.x ABI mismatches.

    None values are converted to the string "NA_SENTINEL_" so that
    R's as.data.frame() sees consistent column lengths.  The R side
    replaces these sentinels with proper NA values.

    Args:
        session: Snowpark Session object.
        sql: SQL query string.

    Returns:
        dict with keys:
            - "columns": list of column names
            - "data": dict[str, list] of column name → values
            - "nrows": int
    """
    _NA = "NA_SENTINEL_"

    pdf = session.sql(sql).to_pandas()
    # Strip surrounding quotes from column names (SHOW/DESCRIBE commands
    # return quoted identifiers like '"name"' which R's make.names mangles)
    clean_cols = [c.strip('"') for c in pdf.columns]
    pdf.columns = clean_cols
    cols = list(clean_cols)
    nrows = len(pdf)

    if nrows == 0:
        return {"columns": cols, "data": {c: [] for c in cols}, "nrows": 0}

    import datetime

    # Use .tolist() to convert numpy scalars to native Python types
    # (int, float, str).  This avoids the NumPy ABI mismatch because
    # reticulate never sees numpy array objects.
    # pd.isna() detects all flavours of missing (None, NaN, NaT, pd.NA).
    data = {}
    for col in cols:
        na_mask = pdf[col].isna()
        vals = pdf[col].tolist()

        # Convert datetime objects to ISO strings for clean R output
        if vals and isinstance(vals[0], (datetime.datetime, datetime.date)):
            vals = [v.isoformat() if isinstance(v, (datetime.datetime, datetime.date)) else v for v in vals]

        if na_mask.any():
            data[col] = [_NA if is_na else v for v, is_na in zip(vals, na_mask)]
        else:
            data[col] = vals

    return {"columns": cols, "data": data, "nrows": nrows}
