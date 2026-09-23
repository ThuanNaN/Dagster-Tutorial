"""DuckDB resource for analytical query layer."""
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 8080
    database: str = "mydb.duckdb"

    def get_connection_url(self) -> str:
        return f"http://{self.host}:{self.port}/{self.database}"

    def get_jdbc_url(self) -> str:
        return f"jdbc:duckdb:http://{self.host}:{self.port}/{self.database}"
