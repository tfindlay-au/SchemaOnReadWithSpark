# SchemaOnReadWithSpark

### Overview
This project is aimed at demonstrating the pattern of applying a schema (interpretation of data) on consumption rather than on receipt.

I have seem this pattern used a few times in the past and I think it works well for some use cases. 
One of the interesting parts is applying software engineering principals including:
* Version control
* Unit testing
* CI/CD
* Documentation (ScalaDoc or similar)

These are often easier to do when all of the business rules/logic are contained in a bundle the can be easily unit tested and versioned.

In data pipelines, these are traditionally done (to some degree) with expensive commercial software but rarely done with Open Source tools and DIY pipelines.

| Pro's | Con's|
|----------------------------------------------|--------------------------------------------------|
| * Faster ingestion | * Traceability can be an issue |
| * More resilient ingestion | * Compute cost paid on access |
| * Flexible - The interpretation can change/evolve | |
| * Less data movement / duplication / reconciliation | |
| * Less storage | |

*Note*: These opinions are purely my own and dont reflect those of my employer or any anyone else. They are also subject to change as advancements in tooling change over time and my own experience changes.

![alt Overview](/docs/Overview.jpg)

### Setup
To use a the JAR file produced, you should start with Apache Zeppelin like so:
```
docker run -p 8080:8080 -v /Users/<username>/data/:/data/ --rm apache/zeppelin:0.8.1
```

![alt Diaram](/docs/Diagram.jpg)

*Note*: the directory mapping. This is where your data will be stored.
We will also use the mapped path to copy the JAR into the Zeppelin container.

Once started, point your browser to: http://localhost:8080/

Import `notebooks/SetupData.json` to create some data and setup the project
If you run all paragraphs you should have created a simple parquet file with 3 rows of data.

### Generated Documentation
```
sbt doc
```

Once generated, HTML Scala Doc will be placed under `./target/scala-2.12/api/`

Use your favourite browser to explore. This still requires developer discipline to put this in the code but provides
more than you find in most SQL scripts.


### Testing
```
sbt test
```

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
import MyDataProducts.raw.ProductRawStock
val spark = SparkSession.builder.getOrCreate()
import spark.implicits._
val df = spark.productRawStock()
```

