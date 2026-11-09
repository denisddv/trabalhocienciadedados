import argparse
from pyspark.sql import SparkSession,functions as F
from comum import SCHEMA,normalizar,validar
p=argparse.ArgumentParser()
p.add_argument('--base',default='/opt/project/datalake')
args=p.parse_args()
spark=SparkSession.builder.appName('NovaMarketBatch').getOrCreate()
raw=spark.read.schema(SCHEMA).json(args.base+'/raw/pedidos')
trusted=validar(normalizar(raw)).dropDuplicates(['id_pedido'])
trusted.write.mode('overwrite').parquet(args.base+'/trusted/pedidos')
refined=(spark.read.parquet(args.base+'/trusted/pedidos')
  .groupBy('categoria').agg(F.count('*').alias('pedidos'),F.round(F.sum('valor_total'),2).alias('faturamento'),F.round(F.avg('valor_total'),2).alias('ticket_medio')))
refined.write.mode('overwrite').parquet(args.base+'/refined/vendas_por_categoria')
print('VALIDACAO',raw.count(),trusted.count(),refined.count())
refined.explain(True)
spark.stop()
