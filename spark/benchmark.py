import json,os,time,argparse
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.functions import broadcast
p=argparse.ArgumentParser()
p.add_argument('--saida',default='/opt/project/benchmark/resultados.json')
p.add_argument('--linhas',type=int,default=300000)
args=p.parse_args()
spark=(SparkSession.builder.appName('NovaMarketBenchmark').config('spark.sql.autoBroadcastJoinThreshold','-1').config('spark.sql.adaptive.enabled','false').config('spark.sql.shuffle.partitions','8').getOrCreate())
pedidos=spark.range(args.linhas).selectExpr('id','cast(id % 4 as int) as categoria_id','cast(id % 1000 as double) as valor').cache()
pedidos.count()
categorias=spark.createDataFrame([(0,'casa'),(1,'livros'),(2,'moda'),(3,'eletronicos')],['categoria_id','categoria'])
resultados={}
for nome,dim in [('antes_shuffle_join',categorias),('depois_broadcast_join',broadcast(categorias))]:
    inicio=time.perf_counter()
    rows=pedidos.join(dim,'categoria_id').groupBy('categoria').agg(F.sum('valor').alias('total')).collect()
    resultados[nome]={'segundos':round(time.perf_counter()-inicio,4),'categorias':len(rows),'total':round(sum(r['total'] for r in rows),2)}
resultados['ganho_percentual']=round(100*(resultados['antes_shuffle_join']['segundos']-resultados['depois_broadcast_join']['segundos'])/resultados['antes_shuffle_join']['segundos'],2)
os.makedirs(os.path.dirname(args.saida),exist_ok=True)
with open(args.saida,'w') as f:json.dump(resultados,f,indent=2)
print(json.dumps(resultados,indent=2))
spark.stop()
