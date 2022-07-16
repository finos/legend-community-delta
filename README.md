# Legend on Delta Lake

*An extension to the [Legend](https://legend.finos.org/) framework for spark / delta lake based environment, combining
best of open data standards with open source technologies*

[![FINOS - Incubating](https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg)](https://finosfoundation.atlassian.net/wiki/display/FINOS/Incubating)
[![Build CI](https://github.com/finos/legend-engine/workflows/Build%20CI/badge.svg)]()
[![Maven Central](https://img.shields.io/maven-central/v/org.finos.legend-community/legend-delta.svg)](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22legend-delta)

___

In addition to the JDBC connectivity enabled to Databricks from the 
[legend-engine](https://github.com/finos/legend-engine/tree/master/docs/databricks) itself, this project helps 
organizations define data models that can be converted into efficient data pipelines, ensuring data being queried
is of high quality and availability. Raw data can be ingested as stream or batch and processed in line with the 
business semantics defined from the Legend interface. Domain specific language defined in Legend Studio can be 
interpreted as a series of Spark SQL operations, helping analysts create [Delta Lake](https://delta.io/) tables that 
not only guarantees schema definition but also complies with expectations, derivations and constraints defined by 
business analysts.

<img src="https://raw.githubusercontent.com/finos/legend-community-delta/main/images/legend-delta-workflow.png" width="500">

## Usage

Make sure to have the jar file of `org.finos.legend-community:legend-delta:X.Y.Z` and all its dependencies available in
your spark classpath and a legend data model (version controlled on gitlab) previously compiled to disk or packaged
as a jar file and available in your classpath. For python support, please add the corresponding library from pypi
[repo](https://pypi.org/project/legend-delta/).

```shell
pip install legend-delta==X.Y.Z
```

We show you how to extract schema, retrieve and enforce expectations and create delta tables in both
[scala](https://github.com/finos/legend-delta/blob/main/databricks-scala.ipynb) and 
[python](https://github.com/finos/legend-delta/blob/main/databricks-python.ipynb) sample notebooks.

## Author

Databricks, Inc.

## License

Copyright 2021 Databricks, Inc.

Distributed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

SPDX-License-Identifier: [Apache-2.0](https://spdx.org/licenses/Apache-2.0)
