# CCA 175 Prep

## Sqoop
### Import data from MySQL

```shell

sqoop import \
    --connect <jdbc-uri> \ jdbc:mysql://localhost:3306/retail_db
    --password <password> \
    --username <username> \
    --target-dir \

    --table <table_name> \
    --query <sql_query> \
    --where <where clause> \
    --columns <col1,col2,...> \    

    --as-avrodatafile \
    --as-parquetfile \
    --as-textfile \ (default)
    --m <number_of_mappers> \

    --enclosed-by <field_enclosing_char> \
    --escaped-by <escape_char> \
    --fields-terminated-by <char> \
    --lines-terminated-by <char> \
    
    # Can also create Hive tables based on the sqoop import
    --hive-import
    # Can also import into HBase


sqoop import-all-tables \
    --warehouse-dir <dir> 

# For testing
sqoop eval \
    --query <statement>

```

### Export data from MySQL

```shell

sqoop export \
    --connect <jdbc-uri> \ jdbc:mysql://localhost:3306/retail_db
    --password <password> \
    --username <username> \

    --columns <col1,col2,...>
    --export-dir
    --table <table_name>
    --m <number_of_mappers>

    --update-mode <updateonly(default)|allowinsert>
    --update-key <id>

    --input-enclosed-by <field_enclosing_char> \
    --input-escaped-by <escape_char> \
    --input-fields-terminated-by <char> \
    --input-lines-terminated-by <char> \

```
Note: only from plain text, parquet/avro has to go through metastore tables or HBase!


## Spark

Spark 1.6.0 is on the exam.

* SQL Context: sqlContext
* Spark Context: sc


### Configuration

```shell

spark-submit --master yarn \
    --class <MAIN CLASS NAME> \ # Only for Scala/Java
    --name <NAME> \
    --jars <extra,jars,..> \
    --py-files <python,files> \ # Only for python
    --files <FILES>
    --conf KEY=VALUE
    <APP-JAR | PYTHON-FILE>
    
```

```shell
pyspark --master yarn

spark-shell --master yarn

```

### Scala
Note the equivalence
```scala
(row => row.slice(0,1))

// is the same as

(_.slice(0,1))

```

```scala
( (v1, v2) => v1 * v2 )

// is the same as

( _ * _ )

```


### Input
```python
data = sqlContext.read.format("format").load("file_location_or_dir")

data = sc.textFile("file")
```

```scala
val data = sqlContext.read.format("format").load("file_location_or_dir");

val data = sc.textFile("file")
```

Note: `sqlContext.read.<>` returns a DataFrame. `sc.<>` returns an RDD.
#### CSV
```python
data = sc.textFile('file_or_dir') \
    .map(lambda line: line.split('seperator')) \
    .map(lambda values: values """map any values to the correct type if needed""")
```

```scala
val data = sc.textFile('file_or_dir') \
    .map(_.split('seperator'))
    .map(values => values /* map any values to the correct type if needed */ )
```

#### Parquet
```python
data = sqlContext.read.parquet('parquet_file_or_dir')
```

```scala
val data = sqlContext.read.parquet("parquet_file_or_dir")
```

#### JSON
```python
data = sqlContext.read.json('json_file_or_dir')
```

```scala
val data = sqlContext.read.json("json_file_or_dir")
```

#### Avro
```python
data = sqlContext.read.format('com.databricks.spark.avro').load('avro_file_or_dir')
```

```scala
val data = sqlContext.read.format("com.databricks.spark.avro").load("avro_file_or_dir")

//OR
import com.databricks.spark.avro._

sqlContext.read.avro("avro_file_or_dir")
```

#### Sequence File (only Java or Scala)
```scala
val data = sc.sequenceFile("location", <class of key>, <class of values>)
```

#### Hive Metastore
```python
sqlContext.sql("USE <db_name>")
data = sqlContext.sql("SELECT * FROM <table_name>")

#OR
data = sqlContext.read.table("<table_name>")

```

```scala
val data = sqlContext.sql("SELECT * FROM <table_name>")
```


### Output
```python
data.write.format("format").save("file_location_or_dir")

data.saveAsTextFile("file")
```

```scala
data.write.format("format").save("file_location_or_dir");

data.saveAsTextFile("file")
```
Note: `data.write.<>` requires data to be a DataFrame. `data.<saveAs...>` requires data to be an RDD.

#### CSV
```python
data.map(lambda row: [str(value) for value in row]) \
    .map(lambda row: "seperator".join(row))
    .saveAsTextFile("dir_location")
```
Note: In case of a UnicodeEncodeError in python you can try 'value.encode("utf-8")' instead of 'str(value)' or apply the str function only to the numeric types.

```scala
data.map(_.mkString('seperator')).saveAsTextFile("dir_location")
//OR
data.map(row => row.mkString('seperator')).saveAsTextFile("dir_location")
```

#### Parquet
```python
# optional, default is gzip
sqlContext.setConf('spark.sql.parquet.compression.codec', '<codec>')

data.write.parquet('location_dir')
```

```scala
// optional, default is gzip
sqlContext.setConf("spark.sql.parquet.compression.codec", "<codec>")

data.write.parquet("location_dir")
```

#### JSON
```python
data.write.json('location_dir')
```

```scala
data.write.json("location_dir")
```

#### Avro
```python
# optional, default is snappy
sqlContext.setConf("spark.sql.avro.compression.codec","<codec>") 
sqlContext.setConf("spark.sql.avro.deflate.level", "5")

data.write.format('com.databricks.spark.avro').save('location_dir')

```

```scala
// optional, default is snappy
sqlContext.setConf("spark.sql.avro.compression.codec","<codec>") 
sqlContext.setConf("spark.sql.avro.deflate.level", "5")

data.write.format("com.databricks.spark.avro").save("location_dir")

//OR
import com.databricks.spark.avro._

data.write.avro("location_dir")
```

#### Sequence File (only Java or Scala)
```scala
// data must be a Key-Value RDD
data.saveAsSequenceFile("dir_location")
```

#### Hive Metastore
```python
//PERMANENT
data.write.saveAsTable("table_name")

//TEMPORARY
data.registerTempTable("table_name")
```

```scala
//PERMANENT
data.write.saveAsTable("table_name")

//TEMPORARY
data.registerTempTable("table_name")
```

### Transformations
For **SQLContext**, use the following system:

```python
sqlContext.read.format("<format>").load("file_or_dir").registerTempTable("<table_name>")

target = sqlContext.sql("SELECT <c1, c2> FROM <table_name> WHERE <condition> GROUP BY ... )")

target.write.format("<format>").save("target_dir")
```

It is way simpler then trying to figure out the different methods on the Dataframe (if you know SQL/HiveQL).

DataFrame:
```python
# Selecting a subset of the columns can be done in multiple ways
df.select('col1', 'col2', ...)
df.select(df['col1'])
df.select(df.col1)

# Filter
df.where('col1 > 10') # the same as df.filter('col1 > 10')

# Sort
df.sort(df['col1'].desc())
df.sort(df['col1'].asc())

# Join
df1.join(df2, "join_col_name")

# Print first n rows (default n=20)
df.show(n)
```

Changing between DataFrame and RDD:
```python
# DataFrame to RDD of Row objects
df.rdd 
# DataFrame to pair RDD
df.map(lambda Row: (Row.col1, Row.col2))

# RDD to DataFrame with known schema
sqlContext.createDataFrame(rdd, df.schema)
# RDD to DataFrame by infering the schema from the first row
rdd_row = rdd.map(lambda p: Row(name=p[0], age=int(p[1])))
sqlContext.createDataFrame(rdd_row)
```


RDD:

```python
# Data is already loaded into an RDD: data

# Filter: only keep row where value of 3rd field is lower then 5
data.filter(lambda row: row[2] < 5)

# Map: multiply 3rd and 4th field in a row
data.map(lambda row: row[:2] ++ [row[2] * row[3]] ++ row[4:])

# Join: data_one and data_two on 1st column
data_one = data_one.map(lambda row: (row[0], row))
data_two = data_two.map(lambda row: (row[0], row))

inner_joined_data = data_one.join(data_two)
left_outer_joined_data = data_one.leftOuterJoin(data_two)

# Group By
data.map(lambda row: (row[<key_index>], row)).groupByKey() 
data.groupByKey() # if data already a key value RDD

# Aggregate by key
data.map(lambda row: (row[<key_index>], row[<value_key>])).reduceByKey(<reduction_function>) 
data.map(lambda row: (row[<key_index>], 1)).reduceByKey(<reduction_function>) # for counting
data.reduceByKey(<reduction_function>)  # if data already a key value RDD

# Aggregate in general
data.reduce(<reduction_function>)

# Sort: sort on second field, ascending
data.sortBy(lambda row: row[1])

# Sort: sort on second field, descending
data.sortBy(lambda row: - row[1])
```

```scala
// Data is already loaded into an RDD: data

// Filter: only keep row where value of 3rd field is lower then 5
data.filter(_(2) < 5)

// Map: multiply 3rd and 4th field in a row
data.map(row => row.slice(0,2) ++ Array(row(2) * row(3)) ++ row.slice(4,12))

// Join: data_one and data_two on 1st column
data_one = data_one.map(lambda row: (row(0), row))
data_two = data_two.map(lambda row: (row(0), row))

val inner_joined_data = data_one.join(data_two)
val left_outer_joined_data = data_one.leftOuterJoin(data_two)

// Group By
data.map(row => (row(<key_index>), row)).groupByKey() 
data.groupByKey() # if data already a key value RDD

// Aggregate by key
data.map(row => (row(<key_index>), row(<value_key>))).reduceByKey(<reduction_function>) 
data.map(_(<key_index>), 1)).reduceByKey(<reduction_function>) # for counting
data.reduceByKey(<reduction_function>)  # if data already a key value RDD

// Aggregate in general
data.reduce(<reduction_function>)

// Sort: sort on second field, ascending
data.sortBy(_(1))

// Sort: sort on second field, descending
data.sortBy(- _(1))
```

Commands for checking the output files:
```python
# Text
hdfs dfs -tail <hdfs_file> 

#Avro and Parquet
hdfs dfs -get <hdfs_file> <local_filename>

avro-tools tojson <local_file.avro> | tail
avro-tools getmeta <local_file.avro>
avro-tools getschema <local_file.avro>

parquet-tools head <local_file.parquet>
parquet-tools meta <local_file.parquet>
parquet-tools schema <local_file.parquet>
```



