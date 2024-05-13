Pyspark
```
spark.catalog.listTables()
```

```
import pyspark.sql.functions as F

df = spark.read.table(<dataframe>)

df
  .filter(!col('id').isnull())
  .withColumn('countryCode',lit('USA'))
  .withColumnRenamed('geo','region')
```

```
raw_data = sc.textFile("daily_show.tsv")
raw_data.take(5)

daily_show = raw_data.map(lambda line: line.split('\t'))
daily_show.take(5)
```

value_counts
```
// python
tally = dict()
for line in daily_show:
  year = line[0]
  if year in tally.keys():
    tally[year] = tally[year] + 1
  else:
    tally[year] = 1

// scala
tally = daily_show.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)
tally.take(5)
tally.take(tally.count())
```

filter
```
def filter_year(line):
    # Write your logic here
    if line[0] != 'YEAR':
        return True

filtered_daily_show = daily_show.filter(lambda line: filter_year(line))

filtered_daily_show.take(3)

```

```
filtered_daily_show.filter(lambda line: line[1] != '') \
                   .map(lambda line: (line[1].lower(), 1)) \
                   .reduceByKey(lambda x,y: x+y) \
                   .take(5)
```
