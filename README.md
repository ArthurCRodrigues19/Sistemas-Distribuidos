# TDE 03 - Detecção de Fraude com Kafka

O `producer.py` envia uma transação aleatória para o Kafka a cada 3 segundos.
O `consumer.py` lê as transações, verifica se é fraude, salva no banco (SQLite) e mostra no console.

Exemplo de transação:

```json
{"transaction_id": "3bb7d9f2-...", "client_id": "cliente_5", "amount": 1994.97, "city": "São Paulo", "timestamp": "2026-10-13T18:35:07"}
```

## Regras de fraude

- **ALTO_VALOR**: transação com valor >= 10000
- **TEMPO_60s**: mesmo cliente com 4 transações em menos de 60 segundos
- **GEO_10m**: mesmo cliente com transações em cidades diferentes em 10 minutos

## Como rodar

```
docker compose up -d
pip install -r requirements.txt
python consumer.py
python producer.py   (em outro terminal)
```

Para ver o que foi salvo no banco: `python consultar_banco.py`

Para parar: `Ctrl+C` e depois `docker compose down -v`
