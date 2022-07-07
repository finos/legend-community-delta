<img width="20%" src="https://github.com/finos/legend-delta/blob/main/images/legend-delta.png?raw=true.png">

# legend-delta

In addition to the JDBC connectivity enabled to Databricks from the [legend-engine](https://github.com/finos/legend-engine/tree/master/docs/databricks) itself, 
this project helps organizations define data models that can be converted into efficient data pipelines, ensuring data being queried
is of high quality and availability. Raw data can be ingested as stream or batch and processed in line with the business semantics 
defined from the Legend interface. Domain specific language defined in Legend Studio can be interpreted as a series of Spark SQL operations,
helping analysts create [Delta Lake](https://delta.io/) tables that not only guarantees schema definition but also complies
with expectations, derivations and constraints defined by business analysts.

<img src="https://github.com/finos/legend-delta/blob/main/images/legend-delta-workflow.png?raw=true" width="500">

## Usage

Make sure to have the jar file of `legend-delta` and its dependencies available in your classpath and a legend data model 
(version controlled on gitlab) previously synchronized on disk or packaged as jar and available in your classpath.
We show you below how to extract schema, retrieve and enforce expectations.

See example notebook in scala [here](https://github.com/finos/legend-delta/blob/main/databricks-scala.ipynb). 
For convenience, a python wrapper module was created to offer similar experience to pyspark users. 
See example notebook [here](https://github.com/finos/legend-delta/blob/main/databricks-python.ipynb).

## Author

Databricks, Inc.

## License

Copyright 2021 Databricks, Inc.

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
