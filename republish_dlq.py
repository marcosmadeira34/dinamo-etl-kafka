#!/usr/bin/env python3
"""
Republica mensagens de sped.errors.dlq de volta ao topico de origem
(original_topic), filtrando por stream e/ou timestamp de falha.

Uso (dry-run, so mostra o que seria feito):
    python3 republish_dlq.py --stream silver-stream

Uso real (publica de fato):
    python3 republish_dlq.py --stream silver-stream --execute

Filtra opcionalmente por incidente especifico (evita repescar DLQ de
incidentes futuros nao relacionados):
    python3 republish_dlq.py --stream silver-stream --failed-at 2026-07-23T20:34:40.500Z --execute
"""
import argparse
import json
import os
import sys

from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka.dinamo-kafka.svc.cluster.local:9092")
DLQ_TOPIC = "sped.errors.dlq"


def read_all_dlq_messages(bootstrap: str):
    """Le TODAS as mensagens do DLQ do inicio ao fim (topico pequeno, ok fazer seek+poll ate o fim)."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"dlq-republish-inspect-{os.getpid()}",  # group descartavel, nao interfere em nada
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    md = consumer.list_topics(DLQ_TOPIC, timeout=10)
    partitions = [
        TopicPartition(DLQ_TOPIC, p)
        for p in md.topics[DLQ_TOPIC].partitions.keys()
    ]
    consumer.assign(partitions)

    end_offsets = {}
    for tp in partitions:
        _, high = consumer.get_watermark_offsets(tp, timeout=10)
        end_offsets[tp.partition] = high

    messages = []
    empty_polls = 0
    while empty_polls < 5:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        messages.append(msg)
        # para quando ja lemos ate o fim de todas as particoes
        current = consumer.position([TopicPartition(DLQ_TOPIC, msg.partition())])[0]
        if current.offset >= end_offsets.get(msg.partition(), 0):
            empty_polls = 0
    consumer.close()
    return messages


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", help="Filtra por _failed_stream (ex: silver-stream) — so existe em mensagens gravadas apos a correcao que adicionou esse campo")
    parser.add_argument("--failed-at", help="Filtra por _failed_at exato (ex: 2026-07-23T20:34:40.500Z) — mesma ressalva acima")
    parser.add_argument("--sped-type", help="Filtra por _sped_type (ex: EFD_CONTRIB) — existe em TODAS as mensagens, inclusive as antigas")
    parser.add_argument("--status", help="Filtra por status (ex: PROCESSING_FAILED) — existe em TODAS as mensagens, inclusive as antigas")
    parser.add_argument("--execute", action="store_true", help="Sem essa flag, so mostra o que seria republicado (dry-run)")
    args = parser.parse_args()

    messages = read_all_dlq_messages(BOOTSTRAP)
    print(f"Total de mensagens no DLQ: {len(messages)}")

    candidatos = []
    for msg in messages:
        try:
            envelope = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            print(f"  [skip] partition={msg.partition()} offset={msg.offset()} — nao consegui parsear envelope: {e}")
            continue

        if args.stream and envelope.get("_failed_stream") != args.stream:
            continue
        if args.failed_at and envelope.get("_failed_at") != args.failed_at:
            continue
        if args.sped_type and envelope.get("_sped_type") != args.sped_type:
            continue
        if args.status and envelope.get("status") != args.status:
            continue

        original_topic = envelope.get("original_topic")
        raw_value = envelope.get("kafka_value_truncated")
        key = msg.key().decode("utf-8") if msg.key() else None

        if not original_topic or not raw_value:
            print(f"  [skip] partition={msg.partition()} offset={msg.offset()} — envelope incompleto")
            continue

        # valida que o valor original nao foi de fato truncado (4096 bytes) —
        # se bateu no limite, republicar geraria dado incompleto/corrompido.
        if len(raw_value.encode("utf-8")) >= 4096:
            print(f"  [ATENCAO] key={key} pode estar truncado (>=4096 bytes) — NAO sera republicado automaticamente")
            continue

        try:
            json.loads(raw_value)  # valida que o JSON original esta integro
        except Exception:
            print(f"  [skip] key={key} — kafka_value_truncated nao e JSON valido")
            continue

        candidatos.append({
            "key": key,
            "value": raw_value,
            "original_topic": original_topic,
            "_file_id": envelope.get("_failed_stream"),
            "error": envelope.get("_error_message", "")[:100],
        })

    print(f"\nCandidatos a republicar: {len(candidatos)}")
    for c in candidatos:
        print(f"  -> topic={c['original_topic']} key={c['key']} | erro original: {c['error']}...")

    if not args.execute:
        print("\n[DRY-RUN] Nada foi publicado. Rode de novo com --execute pra publicar de verdade.")
        return

    if not candidatos:
        print("\nNada a republicar.")
        return

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
    })

    publicados = 0
    falhas = []

    def delivery_report(err, msg):
        nonlocal publicados
        if err is not None:
            falhas.append(str(err))
        else:
            publicados += 1

    for c in candidatos:
        producer.produce(
            topic=c["original_topic"],
            key=c["key"],
            value=c["value"].encode("utf-8"),
            callback=delivery_report,
        )
    producer.flush(30)

    print(f"\nRepublicadas com sucesso: {publicados}/{len(candidatos)}")
    if falhas:
        print("Falhas:")
        for f in falhas:
            print(f"  - {f}")
        sys.exit(1)


if __name__ == "__main__":
    main()