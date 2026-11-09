"""Comparação experimental entre shuffle join e broadcast join (NovaMarket)."""
import argparse
import json
import os
import tempfile
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

parser = argparse.ArgumentParser()
parser.add_argument("--saida", default="/opt/project/benchmark/resultados.json")
parser.add_argument("--linhas", type=int, default=300000)
args = parser.parse_args()
if args.linhas <= 0:
    parser.error("--linhas deve ser maior que zero")

spark = (
    SparkSession.builder
    .appName("NovaMarketBenchmark")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

try:
    pedidos = (
        spark.range(args.linhas)
        .selectExpr(
            "id",
            "cast(id % 4 as int) as categoria_id",
            "cast(id % 1000 as double) as valor",
        )
        .cache()
    )
    pedidos.count()  # materializa cache antes das duas medições
    categorias = spark.createDataFrame(
        [(0, "casa"), (1, "livros"), (2, "moda"), (3, "eletronicos")],
        ["categoria_id", "categoria"],
    )

    resultados = {}
    for nome, dimensao in [
        ("antes_shuffle_join", categorias),
        ("depois_broadcast_join", broadcast(categorias)),
    ]:
        inicio = time.perf_counter()
        linhas = (
            pedidos.join(dimensao, "categoria_id")
            .groupBy("categoria")
            .agg(F.sum("valor").alias("total"))
            .collect()
        )
        resultados[nome] = {
            "segundos": round(time.perf_counter() - inicio, 4),
            "categorias": len(linhas),
            "total": round(sum(linha["total"] for linha in linhas), 2),
        }

    antes = resultados["antes_shuffle_join"]
    depois = resultados["depois_broadcast_join"]
    if (antes["categorias"], antes["total"]) != (depois["categorias"], depois["total"]):
        raise ValueError("As duas estratégias produziram resultados diferentes")

    resultados["ganho_percentual"] = round(
        100 * (antes["segundos"] - depois["segundos"]) / antes["segundos"], 2
    ) if antes["segundos"] > 0 else None

    diretorio = os.path.dirname(os.path.abspath(args.saida))
    os.makedirs(diretorio, exist_ok=True)
    temporario = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=diretorio,
            prefix=".novamarket_", suffix=".json", delete=False,
        ) as arquivo:
            temporario = arquivo.name
            json.dump(resultados, arquivo, indent=2, ensure_ascii=False)
            arquivo.write("\n")
        os.chmod(temporario, 0o664)  # leitura para o usuário do VS Code e grupo Spark
        os.replace(temporario, args.saida)
        temporario = None
    finally:
        if temporario is not None and os.path.exists(temporario):
            os.remove(temporario)

    print(json.dumps(resultados, indent=2, ensure_ascii=False))
finally:
    spark.stop()
