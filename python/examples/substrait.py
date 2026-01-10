import duckdb

con = duckdb.connect()
con.install_extension("substrait", repository = "community")
con.load_extension("substrait")