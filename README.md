[![FINOS - Incubating](https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg)](https://finosfoundation.atlassian.net/wiki/display/FINOS/Incubating)

# legend-delta

<img width="30%" src="images/legend-delta.png">

Part of the FINOS open source foundation, the [Legend](https://legend.finos.org/) framework is a flexible platform that 
offers solutions to explore, define, connect, and integrate data into your business processes. The Legend language is 
an immutable functional language based on the Unified Modeling Language (UML) and inspired by Object Constraint Language (OCL). 
It provides an accelerated data modeling experience that enables execution of model queries in various environments.

Part of the Linux foundation, [Delta Lake](https://delta.io/) is an open source storage layer that brings reliability to data lakes. 
Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. 
Running on top of your existing data lake and fully compatible with the Apache Spark APIs, Delta brings the best of both 
data warehousing and data lakes functionalities into one unified platform.

By integrating the Legend Framework in a Lakehouse architecture, [Databricks](https://databricks.com/) and FINOS bring 
the best of open data standard and open source technologies together, improving data exchange across the financial services 
industry and supporting key organizations in their digital transformation. Today, we are pleased to release `legend-delta`, 
an open source component that acts as a conduit from domain expertise to automated data pipeline orchestration.

## Overview

In addition to the JDBC connectivity enabled to Delta Lake from the [legend-engine](https://github.com/finos/legend-engine) itself, 
this project helps organizations define data models that can be converted into efficient data pipelines, ensuring data being queried
is of high quality and availability. Raw data can be ingested as stream or batch and processed in line with the business semantics 
defined from the Legend interface.

<img src="images/legend-delta-workflow.png" width="800">

Domain specific language defined in Legend Studio can be interpreted as a series of Spark SQL operations, 
helping analysts create Delta table that not only guarantees schema definition but also complies 
with expectations, derivations and constraints defined by business analysts. 

<img src="images/legend-studio.png" width="800">

Defining entities using the [FIRE](https://suade.org/fire/) data model as an example for regulatory reporting
(PURE code is available [here](src/test/resources/model.pure) for reference), we demonstrate how tables and expectations
can be dynamically generated on Delta Lake to ensure data quality, availability and compliance.

## Usage

Make sure to have the jar file of `legend-delta` available in your classpath and a legend data model 
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
val legend = LegendFileLoader.loadResources(("/path/to/legend/datamodel"))
```

All legend entities available will be retrieved and can be listed as follows, 
expressed in the form of `namespace::entity` and referencable as such.

```scala
legend.getEntityNames.foreach(println)
```

```
fire::currency_code
fire::regulatory_book
fire::collateral
fire::collateral_type
fire::encumbrance_type
```

### Convert pure to delta format

With our legend entities loaded, we can create the Delta schema for any entity of type `Class`. 
This process will recursively loop through each of its underlying fields, enums and possibly nested properties and supertypes.

```scala
val schema = legend.getEntitySchema("fire::collateral")
```

Note that we do not only find fields and their data types, but also retrieve Legend `TaggedValues` 
as business metadata (field description). One can simply create a delta table using the following `schema.toDDL` syntax.

```
`id` STRING COMMENT 'The unique identifier for the collateral within the financial institution.'
`date` TIMESTAMP,`loan_ids` ARRAY<STRING> COMMENT 'The unique identifiers for the loans within the financial institution.'
`account_ids` ARRAY<STRING> COMMENT 'The unique identifier/s for the account/s within the financial institution.'
`charge` INT COMMENT 'Lender charge on collateral, 1 indicates first charge, 2 second and so on. 0 indicates a combination of charge levels.'
`currency_code` STRING COMMENT 'The currency of the contribution_amount in accordance with ISO 4217 standards.'
`encumbrance_amount` INT COMMENT 'The amount of the collateral that is encumbered by potential future commitments or legal liabilities. Monetary type represented as a naturally positive integer number of cents/pence.'
`encumbrance_type` STRING COMMENT 'The type of the encumbrance causing the encumbrance_amount.'
`end_date` TIMESTAMP COMMENT 'The end date for recognition of the collateral'
`regulatory_book` STRING COMMENT 'The type of portfolio in which the instrument is held.'
`source` STRING COMMENT 'The source(s) where this data originated. If more than one source needs to be stored for data lineage, it should be separated by a dash. eg. Source1-Source2'
`start_date` TIMESTAMP COMMENT 'The start date for recognition of the collateral'
`type` STRING COMMENT 'The collateral type defines the form of the collateral provided (should not be null)'
`value` INT COMMENT 'The valuation as used by the bank for the collateral on the value_date. Monetary type represented as a naturally positive integer number of cents/pence.'
`value_date` TIMESTAMP COMMENT 'The timestamp that the collateral was valued',`version_id` STRING COMMENT 'The version identifier of the data such as the firm\'s internal batch identifier'
`vol_adj` BIGINT COMMENT 'The volatility adjustment appropriate to the collateral.'
`vol_adj_fx` BIGINT COMMENT 'The volatility adjustment appropriate to currency mismatch.'
```

Data can be schematized "on-the-fly" when reading raw records (see below an example reading JSON files).
Although JSON usually looks structured, imposing schema would guarantee missing fields are still expected
and data types fully enforced (e.g. a date object will be processed as a `java.sql.Date` instead of string)

```scala
val collateral = spark.read.format("json").schema(schema).load("/path/to/collateral/json")
```

### Detecting schema drift

As we dynamically create a Delta Lake schema off a legend entity, we can easily detect schema changes against an existing dataframe. 

```scala
val drift = legend.detectEntitySchemaDrift("fire::collateral", existing_df)
require(drift.isCompatible)
```

Some changes will be considered as non backwards compatible (column was dropped or data type has changed). Others can
be applied using a simple `ALTER` statement. New columms can be added, new metadata can be provided, and changes
from Legend studio can be automatically applied using a CI/CD process (handling changes on Gitlab main branch).

```scala
drift.alterStatements.foreach(println)
```

```
ADD COLUMNS (`id` STRING COMMENT 'The unique identifier for the collateral within the financial institution.')
```

### Retrieve expectations

Inferring the schema is one thing, enforcing its constraints is another. Given the `multiplicity` properties, we can 
detect if a field is optional or not, if list has the right number of elements. Given an `enumeration`, 
we check for value consistency. All these rules are dynamically generated as SQL expressions as follows

```scala
val expectations = legend.getEntityExpectations("fire::collateral")
expectations.foreach(println)
```

```
Expectation(charge_min,Success((`charge`) > (0)))
Expectation(`id` mandatory,Success(`id` IS NOT NULL))
Expectation(`date` mandatory,Success(`date` IS NOT NULL))
Expectation(`loan_ids` multiplicity,Success(`loan_ids` IS NULL OR SIZE(`loan_ids`) >= 0))
Expectation(`account_ids` multiplicity,Success(`account_ids` IS NULL OR SIZE(`account_ids`) >= 0))
Expectation(`currency_code` contains,Success(CASE WHEN `currency_code` IS NOT NULL THEN `currency_code` IN ('AED', ...
Expectation(`encumbrance_amount` mandatory,Success(`encumbrance_amount` IS NOT NULL))
Expectation(`encumbrance_type` contains,Success(CASE WHEN `encumbrance_type` IS NOT NULL THEN `encumbrance_type` IN ('repo', ...
Expectation(`regulatory_book` contains,Success(CASE WHEN `regulatory_book` IS NOT NULL THEN `regulatory_book` IN ('trading_book', ...
Expectation(`type` mandatory,Success(`type` IS NOT NULL))
Expectation(`type` contains,Success(CASE WHEN `type` IS NOT NULL THEN `type` IN ('residential_property', 'commercial_property', ...
Expectation(`value` mandatory,Success(`value` IS NOT NULL))
```

In addition to the rules derived from the schema itself, we also support the conversion of business specific constraints 
from the PURE language to SQL expressions. 

<img src="images/legend-constraints.png" width="500">

The syntax and validity of these expressions (e.g. a field must exist) are 
evaluated against an empty dataframe and marked as `Success` or `Failure` accordingly.
For instance, we ensure a PURE functions defined above can be evaluated against existing fields of valid type. 
Such a validation beyond simple syntax requires a Spark context to be available, indicated via the `validate` flag.

```scala
val expectations = legend.getEntityExpectations("fire::collateral", validate = true)
```

See below an example of some business defined PURE functions automatically converted into Spark SQL constraints. 

```scala
sql = "|$this.legend_complexity->greaterThan($this.legend_efforts)".pure2sql
Assert.assertEquals("(`legend_complexity`) > (`legend_efforts`)", sql)

sql = "|$this.legend_complexity->lessThanEqual(2)".pure2sql
Assert.assertEquals("(`legend_complexity`) <= (2)", sql)

sql = "|OR($this.legend->monthNumber()->equal(1), $this.legend->hour()->lessThan(3))".pure2sql
Assert.assertEquals("((MONTH(`legend`)) = (1)) OR ((HOUR(`legend`)) < (3))", sql)

sql = "|OR(AND($this.legend->monthNumber()->equal(1), $this.legend->hour()->lessThan(3)), $this.legend->minute()->greaterThan(10))".pure2sql
Assert.assertEquals("(((MONTH(`legend`)) = (1)) AND ((HOUR(`legend`)) < (3))) OR ((MINUTE(`legend`)) > (10))", sql)
```

For PURE functions that do not have a direct Spark equivalent, we re-coded business logic as User Defined Functions 
that must be registered on a spark context as follows. 

```scala
import org.finos.legend.spark.functions._
spark.registerLegendUDFs()
```

We can validate all expectations at once on a given dataframe using a Legend implicit class, resulting in the same 
data enriched with an additional column. This column (column name can be specified) contains the name of any breaching 
constraints. Hence, an empty array consists in a fully validated record 

```scala
val validated = df.legendExpectations(expectations)
```

In the example above, we simply explode our dataframe to easily access each and every failed expectation, being schema specific
or business defined.

```
+----+------------------+-------------------+-------------------+-------------+--------------------------------------------------+
|id  |encumbrance_amount|start_date         |end_date           |currency_code|legend                                            |
+----+------------------+-------------------+-------------------+-------------+--------------------------------------------------+
|2003|-18               |2024-03-10 14:55:40|2019-03-17 00:41:42|ILS          |[encumbrance_amount] should be between 0 and 99999|
|2003|-18               |2024-03-10 14:55:40|2019-03-17 00:41:42|ILS          |[start_date] should be before [end_date]          |
|2004|82178             |2048-01-23 13:27:46|2022-04-01 01:38:13|ZWD          |[start_date] should be before [end_date]          |
|2004|82178             |2048-01-23 13:27:46|2022-04-01 01:38:13|ZWD          |`currency_code` contains                          |
|2022|99571             |2034-08-29 04:05:56|2026-04-13 13:57:00|JOD          |[encumbrance_amount] should be between 0 and 99999|
|2022|99571             |2034-08-29 04:05:56|2026-04-13 13:57:00|JOD          |[start_date] should be before [end_date]          |
|2023|16218             |2024-07-07 13:21:46|2041-10-31 15:23:47|ZWD          |`currency_code` contains                          |
|2025|16141             |2037-06-20 07:34:57|2015-03-06 23:02:17|HKD          |[start_date] should be before [end_date]          |
|2026|89305             |2050-01-11 23:31:06|2019-01-14 16:37:11|MNT          |[start_date] should be before [end_date]          |
+----+------------------+-------------------+-------------------+-------------+--------------------------------------------------+
```

### Ingest and validate new records using Auto-loader

With the legend building blocks defined, organizations can easily schematize raw data, 
validate technical and business constraints and persist cleansed records to a business semantic layer in batch or real time. 
Using [Autoloader](https://databricks.com/blog/2020/02/24/introducing-databricks-ingest-easy-data-ingestion-into-delta-lake.html) 
functionality of Delta and high governance standards provided by the Legend ecosystem, 
we can ensure reliability **AND** timeliness of financial data pipeline through a simple code base.
In the example below, we show you how to quickly stitch all those concepts together in a real time pipeline. 

```scala
import org.finos.legend.spark._
import org.finos.legend.spark.functions._

// Registering UDFs
spark.registerLegendUDFs()

// Retrieve specs
val legend = LegendFileLoader.loadResources("/path/to/fire-model")
val entity = "fire::collateral"
val schema = legend.getEntitySchema(entity)
val expectations = legend.getEntityExpectations(entity)

// Processing input JSON files
val inputStream = spark
    .readStream
    .format("json")
    .schema(schema)                   // schematize
    .load("/path/to/input/json")

val outputStream = inputStream
    .legendExpectations(expectations) // validate

// Writing results to a delta table
outputStream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/path/to/checkpoint")
    .start("/path/to/delta")
```

## Installation

```
mvn clean install [-P shaded]
```

To create an uber jar that also includes all required legend dependencies, use the `-Pshaded` maven profile

## Author

Databricks, Inc.

## License

Copyright 2021 Databricks, Inc.

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
