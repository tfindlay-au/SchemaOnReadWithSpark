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
docker run -p 8080:8080 --rm apache/zeppelin:0.8.1
```

One started, point your browser to: http://localhost:8080/

### Building
```
sbt package
```

### Using the JAR
```
z.load("/path/to.jar")
import MyDataProducts.raw.ProductRawMenus
val spark = SparkSession.builder.getOrCreate()
import spark.implicits._
val df = spark.productRawMenus()
```