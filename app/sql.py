from sqlparse import parse as parse_sql
from sqlparse import sql
from dataclasses import dataclass


@dataclass
class SQL:
    operation: str
    columns: list[str]
    table: str
    where: dict[str, str] | None = None

    @classmethod
    def from_query(cls, query: str):
        sql_statement = parse_sql(query)
        tokens = sql_statement[0].tokens
        operation = tokens[0].value.lower()
        if operation == "create":
            table = next(
                token.value
                for token in tokens[1:-2]
                if isinstance(token, sql.Identifier)
            )
            columns = tokens[-1]
            columns = [
                # This will extract the column names
                token.lstrip(" \n\t(").split(" ")[0]
                for token in columns.value.split(",")
                if token not in ["(", ")"]
            ]
            return SQL(operation=operation, table=table, columns=columns)

        elif operation == "select":
            columns = []
            where = {}
            from_idx = next((idx for idx, t in enumerate(tokens) if t.value == "from"))
            # Parse column names
            for token in tokens[1:from_idx]:
                if isinstance(token, sql.Function) or isinstance(token, sql.Identifier):
                    columns.append(token.value)
                elif isinstance(token, sql.IdentifierList):
                    columns += map(lambda t: t.strip(), token.value.split(","))

            # Parse the WHERE clause
            where_token = next((t for t in tokens if isinstance(t, sql.Where)), None)
            if where_token:
                comparison_tokens = [
                    t for t in where_token.tokens if isinstance(t, sql.Comparison)
                ]
                for t in comparison_tokens:
                    key, value = t.value.split("=")
                    where[key.strip()] = value.strip(" '")

            # Skip the whitespace token and get to our table name
            table = tokens[from_idx + 2].value
            return SQL(operation=operation, columns=columns, table=table, where=where)
        else:
            raise Exception(f"Unsupported operation type: {operation}")
