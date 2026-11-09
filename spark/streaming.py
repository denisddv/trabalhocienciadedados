import argparse
from pyspark.sql import SparkSession,functions as F
from comum import SCHEMA,normalizar,validar
p=argparse.ArgumentParser()
p.add_argument('--kafka',default='kafka:9092')
p.add_argument('--base',default='/opt/project/datalake')
args=p.parse_args()
spark=SparkSession.builder.appName('NovaMarketStreaming').getOrCreate()
fonte=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',args.kafka).option('subscribe','novamarket-pedidos').option('startingOffsets','earliest').load())
parsed=fonte.select(F.from_json(F.col('value').cast('string'),SCHEMA).alias('registro')).select('registro.*')
raw=(parsed.writeStream.format('json').option('path',args.base+'/raw/pedidos').option('checkpointLocation',args.base+'/_checkpoints/raw').outputMode('append').start())
limpos=validar(normalizar(parsed))
metricas=(limpos.withWatermark('data_evento','10 minutes').groupBy(F.window('data_evento','5 minutes'),'categoria').agg(F.count('*').alias('eventos'),F.round(F.sum('valor_total'),2).alias('faturamento')))
consulta=(metricas.writeStream.format('console').outputMode('update').option('truncate','false').option('checkpointLocation',args.base+'/_checkpoints/metricas').start())
spark.streams.awaitAnyTermination()
