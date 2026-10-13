"""
Mostra o que está salvo no banco: totais por tipo de fraude e as últimas transações.
(Rode depois que o consumer já tiver processado algumas transações.)
"""
import sqlite3

banco = sqlite3.connect("transacoes.db")

total = banco.execute("SELECT COUNT(*) FROM transacoes").fetchone()[0]
com_fraude = banco.execute("SELECT COUNT(*) FROM transacoes WHERE fraudes <> ''").fetchone()[0]
print(f"Total de transações salvas: {total}")
print(f"Transações com fraude:      {com_fraude}\n")

for tipo in ["ALTO_VALOR", "TEMPO_60s", "GEO_10m"]:
    qtd = banco.execute("SELECT COUNT(*) FROM transacoes WHERE fraudes LIKE ?", (f"%{tipo}%",)).fetchone()[0]
    print(f"  {tipo:<11} {qtd}")

print("\nÚltimas 10 transações:")
for linha in banco.execute("SELECT * FROM transacoes ORDER BY timestamp DESC LIMIT 10"):
    print(linha)
