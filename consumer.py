"""
CONSUMER
Lê as transações do tópico "transacoes", aplica as regras de fraude,
salva tudo no banco (SQLite) e mostra o resultado no console.
"""
import json
import sqlite3
from datetime import datetime, timedelta

from kafka import KafkaConsumer

TOPICO = "transacoes"

# ---------------- Banco de dados ----------------
# SQLite: o banco é só um arquivo (transacoes.db), não precisa instalar nada
banco = sqlite3.connect("transacoes.db")
banco.execute("""
    CREATE TABLE IF NOT EXISTS transacoes (
        transaction_id TEXT PRIMARY KEY,
        client_id      TEXT,
        amount         REAL,
        city           TEXT,
        timestamp      TEXT,
        fraudes        TEXT
    )
""")


# ---------------- Regras de fraude ----------------
def verificar_fraudes(t):
    """Recebe uma transação e devolve a lista de fraudes encontradas (vazia = normal)."""
    fraudes = []
    horario = datetime.fromisoformat(t["timestamp"])

    # Case Alto Valor: amount >= 10000
    if t["amount"] >= 10000:
        fraudes.append("ALTO_VALOR")

    # Case Tempo: mesmo cliente com 4 transações em menos de 60s
    # (busca no banco quantas o cliente já fez no último minuto e soma esta)
    um_minuto_atras = (horario - timedelta(seconds=60)).isoformat()
    qtd = banco.execute(
        "SELECT COUNT(*) FROM transacoes WHERE client_id = ? AND timestamp > ?",
        (t["client_id"], um_minuto_atras),
    ).fetchone()[0]
    if qtd + 1 >= 4:
        fraudes.append("TEMPO_60s")

    # Case GEO: mesmo cliente com transação em OUTRA cidade nos últimos 10 minutos
    dez_minutos_atras = (horario - timedelta(minutes=10)).isoformat()
    qtd = banco.execute(
        "SELECT COUNT(*) FROM transacoes WHERE client_id = ? AND timestamp >= ? AND city <> ?",
        (t["client_id"], dez_minutos_atras, t["city"]),
    ).fetchone()[0]
    if qtd > 0:
        fraudes.append("GEO_10m")

    return fraudes


# ---------------- Kafka ----------------
consumer = KafkaConsumer(
    TOPICO,
    bootstrap_servers="localhost:9092",
    group_id="detector-de-fraude",
    auto_offset_reset="earliest",  # na 1ª vez, lê desde a primeira mensagem do tópico
    value_deserializer=lambda valor: json.loads(valor.decode("utf-8")),
)

print("Consumer iniciado! Aguardando transações...\n")

try:
    for mensagem in consumer:
        t = mensagem.value
        fraudes = verificar_fraudes(t)

        # Salva TODAS as transações no banco (OR IGNORE: se a mesma chegar 2x, não duplica)
        banco.execute(
            "INSERT OR IGNORE INTO transacoes VALUES (?, ?, ?, ?, ?, ?)",
            (t["transaction_id"], t["client_id"], t["amount"], t["city"], t["timestamp"], ", ".join(fraudes)),
        )
        banco.commit()

        # Mostra no console
        detalhes = f"{t['timestamp']} | {t['client_id']:<10} | R$ {t['amount']:>9.2f} | {t['city']}"
        if fraudes:
            print(f"[ALERTA DE FRAUDE] {detalhes}  -->  {', '.join(fraudes)}")
        else:
            print(f"[OK]               {detalhes}")
except KeyboardInterrupt:  # Ctrl+C
    consumer.close()  # avisa o Kafka até onde já leu (offset)
    banco.close()
    print("\nConsumer encerrado.")
