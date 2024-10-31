from pyballista.context import BallistaContext

ballista = BallistaContext("remote", host="10.103.0.25").sql("SELECT 5434535")
print(ballista)