from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

SCHEMA=StructType([
    StructField('id_pedido',StringType()),
    StructField('id_cliente',StringType()),
    StructField('categoria',StringType()),
    StructField('quantidade',IntegerType()),
    StructField('valor_unitario',DoubleType()),
    StructField('campanha',StringType()),
    StructField('event_time',StringType()),
])

def normalizar(df):
    return df.withColumn('data_evento',F.to_timestamp('event_time')).withColumn('valor_total',F.round(F.col('quantidade')*F.col('valor_unitario'),2))

def validar(df):
    return df.filter(F.col('id_pedido').isNotNull() & F.col('categoria').isNotNull() & (F.col('quantidade')>0) & (F.col('valor_unitario')>=0) & F.col('data_evento').isNotNull())
