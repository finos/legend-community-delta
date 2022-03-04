[![FINOS - Incubating](https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg)](https://finosfoundation.atlassian.net/wiki/display/FINOS/Incubating)

<img width="20%" src="images/legend-delta.png">

# legend-delta

In addition to the JDBC connectivity enabled to Delta Lake from the [legend-engine](https://github.com/finos/legend-engine/tree/master/docs/databricks) itself, 
this project helps organizations define data models that can be converted into efficient data pipelines, ensuring data being queried
is of high quality and availability. Raw data can be ingested as stream or batch and processed in line with the business semantics 
defined from the Legend interface. Domain specific language defined in Legend Studio can be interpreted as a series of Spark SQL operations,
helping analysts create Delta table that not only guarantees schema definition but also complies
with expectations, derivations and constraints defined by business analysts.

<img src="images/legend-delta-workflow.png" width="500">

## Usage

Make sure to have the jar file of `legend-delta` and its dependencies available in your classpath and a legend data model 
(version controlled on gitlab) previously synchronized on disk or packaged as jar and available in your classpath.
We show you below how to extract schema, retrieve and enforce expectations.

### Retrieve legend entities

Legend project can be loaded by specifying a parent directory where `entities/${namespace}/${model.pure}` 
model definitions can be found. We load legend namespaces from a classpath or disk as follows

```scala
import org.finos.legend.spark.LegendClasspathLoader
val legend = LegendClasspathLoader.loadResources("datamodel")
```

```scala
import org.finos.legend.spark.LegendFileLoader
val legend = LegendFileLoader.loadResources("/path/to/legend/datamodel")
```

All legend entities available will be retrieved and can be listed as follows, 
expressed in the form of `namespace::entity` and referencable as such.

```scala
legend.getEntityNames.foreach(println)
```

```
databricks::entity::employee
databricks::entity::person
databricks::entity::sme
```

### Convert pure entities to delta format

With our legend entities loaded, we can create the Delta schema for any entity of type `Class`. 
This process will recursively loop through each of its underlying fields, enums and possibly nested properties and supertypes.

```scala
val schema = legend.getEntitySchema("databricks::entity::employee")
schema.fields.foreach(s => println(s.toDDL))
```

Note that we do not only find fields and their data types, but also retrieve Legend `TaggedValues` 
as business metadata (field description). 
```
`firstName` STRING NOT NULL COMMENT 'Person first name'
`lastName` STRING NOT NULL COMMENT 'Person last name'
`birthDate` DATE NOT NULL COMMENT 'Person birth date'
`id` INT NOT NULL COMMENT 'Unique identifier of a databricks employee'
`sme` STRING NOT NULL COMMENT 'Programming skill that person truly masters'
`joinedDate` DATE NOT NULL COMMENT 'When did that person join Databricks'
`highFives` INT NOT NULL COMMENT 'How many high fives did that person get'
```

Data can be schematized "on-the-fly" when reading raw records (see below an example reading CSV files).
Although JSON usually looks structured, imposing schema would guarantee missing fields are still expected
and data types fully enforced (e.g. a date object will be processed as a `java.sql.Date` instead of string)

```scala
val schema = legend.getEntitySchema("databricks::entity::employee")

val schematized = spark
    .read
    .format("csv")
    .schema(schema)
    .load("/path/to/data/csv")
```

Similarly, one can enforce schema on an incoming stream of data. In the example below, we parse and enforce schema
on raw JSON records from a Kafka queue.

```scala
import org.apache.spark.sql.functions._
val schema = legend.getEntitySchema("databricks::entity::employee")

val schematized = spark
    .readStream
    .format("kafka")
    .load()
    .select(
      from_json(
        col("value").cast("string"), 
        schema,
      ).alias("parsed_value")
    )
```

### Retrieve expectations

Inferring the schema is one thing, enforcing its constraints is another. Given the `multiplicity` properties, we can 
detect if a field is optional or not or list has the right number of elements. Given an `enumeration`, 
we check for value consistency. These will be considered **technical expectations**.

```scala
val expectations = legend.getEntityExpectations("databricks::entity::employee")
expectations.foreach({ case (name, constraint) =>
  println(name + "\t" + constraint)
})
```

These will be considered **technical expectations** and expressed in the form of SQL constraints

````
[birthDate] is mandatory    birthDate IS NOT NULL
[sme] has correct values    sme IN ('Scala', 'Python', 'C', 'Java', 'R', 'SQL')
[id] is mandatory           id IS NOT NULL
[joinedDate] is mandatory   joinedDate IS NOT NULL
[project] has correct size  SIZE(project) BETWEEN 1 AND 4
````

In addition to the rules derived from the schema itself, we also support the conversion of business specific constraints
from the PURE language to SQL expressions. See below an example of **business expectations** as defined in the legend
studio interface.

<img src="images/legend-constraints.png" width="500">

This, however, is slightly more complex as we need to generate a legend
execution plan from PURE to SQL against a Databricks runtime. In order to convert PURE constraints into spark SQL 
equivalent, we need to indicate our framework the strategy to convert legend entities into relational table. 
By specifying mapping of type relational, we leverage the legend-engine framework to generate an execution plan 
compatible with a Spark backend.

```scala
val expectations = legend.getMappingExpectations("databricks::mapping::employee_delta")
expectations.foreach({ case (name, constraint) =>
  println(name + "\t" + constraint)
})
```

The corresponding expectations will cover all previous technical constraints in addition to the PURE business logic defined
in the studio interface. For example, constraint `$this.joined_date->dateDiff($this.birth_date,DurationUnit.YEARS) > 20`
(employee must be at least 20 years old) will be converted into the following SQL

```
[birthDate] is mandatory    birthDate IS NOT NULL
[sme] has correct values    sme IN ('Scala', 'Python', 'C', 'Java', 'R', 'SQL')
[id] is mandatory           id IS NOT NULL
[joinedDate] is mandatory   joinedDate IS NOT NULL
[project] has correct size  SIZE(project) BETWEEN 1 AND 4
[age] should be > 20        year(joineddate) - year(birthdate) > 20
```

### Enforce expectations

We can validate all expectations at once on a given dataframe using a Legend implicit class, resulting in the same 
data enriched with an additional column. This column (column name can be specified) contains the name of any breaching 
constraints. Hence, an empty array consists in a fully validated record 

```scala
import org.finos.legend.spark._
val validated = df.legendValidate(expectations)
```

In the example above, we simply explode our dataframe to easily access each and every failed expectation, 
being schema specific or business defined.

```
+----------+---------+----------+---+------+----------+---------+--------------------+
| firstname| lastname| birthdate| id|   sme|joineddate|highfives|              legend|
+----------+---------+----------+---+------+----------+---------+--------------------+
|    Anthia|     Duck|1998-02-08| 10|Python|2015-01-14|      277|[age] should be > 20|
|    Chrysa|  Mendoza|1999-03-19| 18|     R|2019-06-06|      195|[age] should be > 20|
|   Sanders|   Dandie|1999-07-10| 20| Scala|2019-07-31|       77|[age] should be > 20|
|   Yanaton|  Schultz|1999-04-16| 27|Python|2016-07-30|      261|[age] should be > 20|
+----------+---------+----------+---+------+----------+---------+--------------------+
```

### Transform strategy

In addition to business expectations, leveraging Mapping object of legend also help us transform raw entities into their
desired states and target tables. Note that relational transformations on legend only support direct mapping 
(no PURE operations or derived properties) and therefore enforced here as `.withColumnRenamed` operations.

```scala
val transformations = legend.getMappingTransformations("databricks::mapping::employee_delta")
val transformed_df = df.legendTransform(transformations)
```

### Target table

Finally, we can retrieve our target schema and target database to write data to. 

```scala
val table_ddl = legend.getMappingTable("databricks::mapping::employee_delta", ddl = true)
println(table_ddl)
```

The target DDL will contain all necessary fields (with metadata) to populate our delta table 
the legend studio expects.

```roomsql
CREATE TABLE legend.employee
USING DELTA
(
`first_name` STRING NOT NULL COMMENT 'Person first name',
`last_name` STRING NOT NULL COMMENT 'Person last name',
`birth_date` DATE NOT NULL COMMENT 'Person birth date',
`id` INT NOT NULL COMMENT 'Unique identifier of a databricks employee',
`sme` STRING COMMENT 'Programming skill that person truly masters',
`joined_date` DATE NOT NULL COMMENT 'When did that person join Databricks',
`high_fives` INT COMMENT 'How many high fives did that person get'
)
```

In the example below, we chain all of our operations to 

1. acquire data
2. schematize records
3. validate constraints
4. store valid information

```scala

import org.apache.spark.sql.SparkSession
import org.finos.legend.spark._

val spark = SparkSession.active

val legend = LegendClasspathLoader.loadResources("datamodel")
val mapping = legend.getMapping("databricks::mapping::employee_delta")
val inputSchema = legend.getMappingSchema(mapping)
val transformations = legend.getMappingTransformations(mapping)
val constraints = legend.getMappingExpectations(mapping)
val outputTable = legend.getMappingTable(mapping, ddl = false)

val input_df = spark
  .read
  .format("csv")
  .schema(inputSchema)
  .load("/path/to/csv")

val transformed_df = input_df.legendTransform(transformations)
val validated_df = input_df.legendValidate(constraints)

validated_df
  .write
  .format("delta")
  .mode("append")
  .saveAsTable(outputTable)

```

## Installation

```
mvn clean install
```

## Dependencies

The entire project depends on latest changes from legend-engine, legend-sdlc and legend-pure that supports 
Databricks data source. 

## Author

Databricks, Inc.

## License

Copyright 2021 Databricks, Inc.

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
