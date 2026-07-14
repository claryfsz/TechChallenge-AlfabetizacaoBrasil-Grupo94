
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os

def read_table(path):
    table = pq.read_table(path)
    for i, field in enumerate(table.schema):
        if pa.types.is_dictionary(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(field.type.value_type))
    return table.to_pandas()

os.makedirs('gold/csv', exist_ok=True)

df1 = read_table('gold/gold_indicador_municipio')
df1.to_csv('gold/csv/gold_indicador_municipio.csv', index=False)
print('indicador_municipio:', len(df1))

df2 = read_table('gold/gold_meta_vs_resultado')
df2.to_csv('gold/csv/gold_meta_vs_resultado.csv', index=False)
print('meta_vs_resultado:', len(df2))

df3 = pd.read_parquet('gold/gold_evolucao_temporal.parquet')
df3.to_csv('gold/csv/gold_evolucao_temporal.csv', index=False)
print('evolucao_temporal:', len(df3))
