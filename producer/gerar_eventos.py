import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

CATEGORIAS = ['eletronicos', 'livros', 'casa', 'moda']

def gerar_evento(indice, rng, atrasado=False):
    agora = datetime.now(timezone.utc)
    return {
        'id_pedido': f'PED-{indice:08d}',
        'id_cliente': f'CLI-{rng.randint(1,1000):05d}',
        'categoria': rng.choice(CATEGORIAS),
        'quantidade': rng.randint(1,4),
        'valor_unitario': round(rng.uniform(15,1500),2),
        'campanha': rng.choice(['organico','black_friday','email']),
        'event_time': (agora-timedelta(minutes=4 if atrasado else 0)).isoformat()
    }

def main():
    from kafka import KafkaProducer
    p = argparse.ArgumentParser()
    p.add_argument('--bootstrap', default='kafka:9092')
    p.add_argument('--topic', default='novamarket-pedidos')
    p.add_argument('--total', type=int, default=200)
    args = p.parse_args()
    rng = random.Random(2026)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'), acks='all', retries=5)
    ultimo = None
    for i in range(args.total):
        item = gerar_evento(i, rng, atrasado=(i%25==0))
        if i%40==0 and ultimo is not None: item = ultimo.copy()
        producer.send(args.topic, key=item['id_pedido'].encode('utf-8'), value=item)
        ultimo=item
        time.sleep(0.02)
    producer.flush()
    producer.close()
    print(f'Enviados {args.total} eventos')

if __name__=='__main__':
    main()
