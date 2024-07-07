Pyspark

```
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.getOrCreate() // create spark

```

* tables in catalog
```
# Print the tables in the catalog
print(spark.catalog.listTables())
```

* spark.sql
```
spark.sql("""SELECT FROM """)
```

* spark dataframe to pandas
```
spark.sql(query).toPandas()
```

* pandas to spark dataframe
```
spark.createDataFrame(pandas)
```

* create spark dataframe to catalog or sql
```
spark_DF.createOrReplaceTempView('<sql name>')
```

* spark read csv
```
spark.read.csv(file_path, header=True)
```


* create column
```
df = df.withColumn("newCol", df.oldCol + 1)
```

```
spark.catalog.listTables()
```

* spark filter
```
flights.filter("air_time > 120").show()
flights.filter(flights.air_time > 120).show()
```

The difference between .select() and .withColumn() methods is that .select() returns only the columns you specify, while .withColumn() returns all the columns of the DataFrame in addition to the one you defined. It's often a good idea to drop columns you don't need at the beginning of an operation so that you're not dragging around extra data as you're wrangling. In this case, you would use .select() and not .withColumn().


* spark select
```
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

flights.select(flights.air_time/60)
flights.select((flights.air_time/60).alias("duration_hrs"))
flights.selectExpr("air_time/60 as duration_hrs")



```

* spark groupby
```
df.groupBy().min("col").show()

# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == 'PDX').groupBy().min('distance').show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == 'SEA').groupBy().max('air_time').show()

# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()


# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()

# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy('month','dest')

# Average departure delay by month and destination
by_month_dest.avg('dep_delay').show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev('dep_delay')).show()
```

* spark join
```
# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed('faa','dest')

# Join the DataFrames
flights_with_airports = flights.join(airports,on='dest',how='leftouter')

# Examine the new DataFrame
flights_with_airports.show()

```

* spark rename col
```
airports = airports.withColumnRenamed(old,new)
airports = airports.withColumnRenamed('faa','dest')
```


---



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
