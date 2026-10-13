"""
PRODUCER
Gera transações aleatórias e publica (envia) no tópico "transacoes" do Kafka.
Uma nova transação a cada 3 segundos.
"""
import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

TOPICO = "transacoes"

CIDADES = ["Curitiba", "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Porto Alegre"]
CLIENTES = [f"cliente_{i}" for i in range(1, 21)]  # 20 clientes

# Cada cliente "mora" em uma cidade fixa, repetindo a lista de cidades:
# cliente_1 -> Curitiba, cliente_2 -> São Paulo, ..., cliente_6 -> Curitiba, ...
CIDADE_DO_CLIENTE = {cliente: CIDADES[i % len(CIDADES)] for i, cliente in enumerate(CLIENTES)}

# Conexão com o Kafka. Os serializers transformam a chave e o valor em bytes
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda chave: chave.encode("utf-8"),
    value_serializer=lambda valor: json.dumps(valor).encode("utf-8"),
)


def gerar_transacao():
    cliente = random.choice(CLIENTES)

    # 5% das vezes o cliente "viaja" e compra em outra cidade (pode gerar GEO_10m)
    if random.random() < 0.05:
        cidade = random.choice(CIDADES)
    else:
        cidade = CIDADE_DO_CLIENTE[cliente]

    # 10% das vezes a compra é de valor alto (gera ALTO_VALOR)
    if random.random() < 0.10:
        valor = round(random.uniform(10000, 50000), 2)
    else:
        valor = round(random.uniform(10, 5000), 2)

    # Padrão da transação
    return {
        "transaction_id": str(uuid.uuid4()),
        "client_id": cliente,
        "amount": valor,
        "city": cidade,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


print("Producer iniciado! Enviando uma transação a cada 3 segundos (Ctrl+C para parar)\n")

try:
    while True:
        transacao = gerar_transacao()

        # A chave é o client_id: assim as transações do mesmo cliente ficam em ordem
        producer.send(TOPICO, key=transacao["client_id"], value=transacao)
        producer.flush()  # garante que a mensagem foi enviada antes de continuar

        print(f"Enviada: {transacao}")
        time.sleep(3)
except KeyboardInterrupt:  # Ctrl+C
    producer.close()
    print("\nProducer encerrado.")
