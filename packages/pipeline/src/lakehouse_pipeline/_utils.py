def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
