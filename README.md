# SchemaOnReadWithSpark

### Overview
This project is aimed at demonstrating the pattern of applying a schema (interpretation of data) on consumption rather than on reciept.

I have seem this pattern used a few times in the past and I think it addresses certain uses cases very well.

Pro's:
* Faster ingestion
* Flexible - The interpretation can change/evolve

Con's:
* Traceability can be an issue

### Setup
Use a the JAR file produced, you should start with Apache Zeppelin like so:
```
docker run -p 8080:8080 -v /Users/<username>/data/:/data/ --rm apache/zeppelin:0.8.1
```

*Note*: the directory mapping. This is where your data will be stored.
We will also use the mapped path to copy the JAR into the Zeppelin container.

Once started, point your browser to: http://localhost:8080/

Import `notebooks/SetupData.json` to create some data and setup the project
If you run all paragraphs you should have created a simple parquet file with 3 rows of data.

### Building
```
sbt package
mv target/scala-2.12/schemaonreadwithspark_2.12-0.1.jar ~/data/schemaonreadwithspark_2.12-0.1.jar 
```

### Using the JAR
With the JAR available, you can see the sample script by loading `notebooks/DataConsumption.json` into Zeppelin.

The principal is to use code to load a library which will provide the interpretation of the data on disk like so:
```
z.load("/path/to.jar")
import MyDataProducts.raw.ProductRawMenus
val spark = SparkSession.builder.getOrCreate()
import spark.implicits._
val df = spark.productRawMenus()
```