# ETL For JDBC

> **Note:** This project is not a replacement for Spring Batch or Apache Camel. Instead, it is a much faster and simpler library designed to cover the most common ETL use case for JDBC: efficiently populating an OLAP database based on the state of an OLTP database. For advanced workflows, orchestration, or complex integrations, consider using Spring Batch or Apache Camel.

## Overview

**ETL For JDBC** is a lightweight high-perfomance ETL(Extract, Transform, Load) library. 
- Designed for cases where a straightforward ETL process is needed, without the complexity or heavy dependencies of frameworks like Apache Camel or Spring Batch. 
- Ideal for lightweight scheduled jobs/AWS Lambdas/Azure Functions.
- Focuses on efficient batch processing, concurrency, and backpressure handling for JDBC source-to-target data transfers.


## Key Features

- **Performance-Oriented:** Efficient batching, concurrency, and backpressure handling for cases where performance is a priority.
- **Simple Setup:** Configure and run ETL jobs with just a few lines of code.
- **Customizable:** Supports row transformation, batch retry logic, and failure handling.
- **Minimal Dependencies:** Only requires SLF4J for logging.
- **Minimal Dependencies:** GraalVM friendly.


## Comparison with Apache Camel

While Apache Camel is a powerful integration framework, ETL For JDBC offers several advantages for JDBC batch ETL tasks:

- **Fine-Grained Batch Control:** Direct configuration of fetch size, batch size, queue size, retry logic, failure limits, and concurrency. Camel’s batch processing is less customizable and more abstracted.
- **Optimized for JDBC ETL:** Purpose-built for JDBC source-to-target ETL, with built-in row transformation and batch upsert logic. Camel is generic and requires more configuration and custom code for similar JDBC batch operations.
- **Backpressure and Resource Management:** Uses a bounded queue and caller-runs policy to prevent memory overload and apply backpressure, which is not natively handled in Camel’s routing engine.
- **Customizable Error Handling and Retry:** Pluggable exception handling and retry logic tailored for SQL exceptions, including terminal failure and retryable error types. Camel’s error handling is more generic and less focused on JDBC specifics.
- **Threading and MDC Propagation:** Explicit concurrency management and MDC propagation for logging context across threads, which is more manual and less transparent in Camel.
- **Streaming Results:** Returns a `Stream<BatchResult>` for batch processing, allowing for functional-style result handling and integration with Java streams.
- **Minimal Dependencies:** Lightweight and focused, reducing complexity and deployment footprint compared to Camel.

In summary, ETL For JDBC is more efficient, customizable, and purpose-built for JDBC batch ETL scenarios than Apache Camel, which is a general-purpose integration framework.

## Comparison with Spring Batch

Spring Batch is a robust framework for complex batch processing, but ETL For JDBC offers several advantages for straightforward JDBC batch ETL tasks:

- **Minimal Setup:** No need for Spring context, beans, or configuration files. ETL jobs are configured and run directly in code.
- **Direct JDBC Access:** No abstraction layers or domain mapping required. Data is moved directly between JDBC sources and targets.
- **Lightweight:** No dependency injection, job repository, or batch infrastructure. Only SLF4J and JDBC are required.
- **Simple Concurrency:** Direct thread configuration without task executors or throttle limits.
- **Functional Streaming:** Returns a `Stream<BatchResult>` for easy, functional-style result handling.
- **Custom Error Handling:** Pluggable exception and retry logic, tailored for JDBC batch operations.
- **No Domain Classes:** No need to define POJOs for source or target rows; works directly with JDBC result sets.
- **Less Code:** ETL jobs can be set up in a few lines, versus extensive configuration and boilerplate in Spring Batch.

In summary, ETL For JDBC is ideal for simple, high-performance JDBC batch ETL jobs, while Spring Batch is better suited for complex workflows, job orchestration, and advanced batch features.

## Getting Started

Add the following dependency to your Maven project:

```xml
<dependency>
    <groupId>io.github.yilativs.batchbridge4jdbc</groupId>
    <artifactId>etl4jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

### ETL For JDBC

```java
//define DataSources to your source and target databases 
DataSource sourceDS = ...; // your source DataSource
DataSource targetDS = ...; // your target DataSource
//define sql to read from source and write to target
String sourceSql = "SELECT id, name FROM source_table";
//by default columns selected from sourceSQL will be used as parameters for targetSql
String targetSql = "INSERT INTO target_table (id, name) VALUES (?, ?)";

//simple approach with sensible defaults (because of sensible defaults often this is all you need)
ETL.Builder(sourceDS, sourceSql, targetDS, targetSql).build().run();

//alternative approach with all optional parameters defined.
ETL.Builder(sourceDS, sourceSql, targetDS, targetSql)
    .fetchSize(1000) //jdbc fetch size
    .batchSize(5000) //jdbc batch size
    .batchQueueCapacity(100) //capacity of queue for batch processing. Fetching thread will stop reading once queue is full.
    .batchRetryLimit(0) //number of retries for retriable exceptions. See BatchExceptionHandler(e.g. deadlock).
    .timeBetweenRetries(0) //milliseconds between retries.
    .failedBatchLimit(0) //maximum number of failed batches allowed before stopping the process.
    .concurrencyLevel(1) //number of concurrent threads to use for processing batches.
    .timeToWaitOnInterrupt()//milliseconds to wait fo termination of executor service.
    .transformer(params -> new Object[]{params[0], params[1].toString().toUpperCase()}) //transformer (transforms second parameter to upper case)
    .exceptionHandler(e -> false) // defines exception handling, returns true if retry is possible.
    .build()
    .run();
```

### Apache Camel (for comparison)

```java
DataSource sourceD = ...; // your source DataSource
DataSource targetS = ...; // your target DataSource
String sourceSql = "SELECT id, name FROM source_table";
String targetSql = "INSERT INTO target_table (id, name) VALUES (:#id, :#name)";
CamelContext context = new DefaultCamelContext();
context.getRegistry().bind("sourceDataSource", sourceDS);
context.getRegistry().bind("targetDataSource", targetDS);

context.addRoutes(new RouteBuilder() {
    @Override
    public void configure() {
        from("jdbc:sourceDataSource?useHeadersAsParameters=true&statement=" + source)
            .split(body())
            .streaming()
            .batchConsumer(1000) // batch size
            .threads(4) // concurrency level
            // .process(customProcessor) // optional transformer (row mapping/processing)
            // .onException(Exception.class).handled(true).process(customExceptionHandler) // optional exception handler
            .to("jdbc:targetDataSource?useHeadersAsParameters=true&statement=" + targetSql);
    }
});
context.start();
```
Requires context creating, binding, and usage of route expressions.

### Spring Batch (for comparison)

```java
int batchSize = 1000; // batch size
String sourceSql = "SELECT id, name FROM source_table";
String targetSql = "INSERT INTO target_table (id, name) VALUES (?, ?)";
// Sample domain classes
public class SourceRow {
    private Long id;
    private String name;
    // getters and setters
}

public class TargetRow {
    private Long id;
    private String name;
    // getters and setters
}

@Bean
public Job etlJob(JobBuilderFactory jobs, StepBuilderFactory steps) {
    return jobs.get("etlJob")
        .start(steps.get("etlStep")
            .<SourceRow, TargetRow>chunk(batchSize) // batch size
            .reader(jdbcReader(sourceSql)) // uses constant
            .processor(processor()) // transformer (required, but can be pass-through)
            // .processor(customProcessor) // optional transformer
            .writer(jdbcWriter(targetSql)) // uses constant
            .faultTolerant() // enables exception handling features
            // .skip(Exception.class) // optional exception handler: skip logic
            // .retry(Exception.class) // optional exception handler: retry logic
            .taskExecutor(new SimpleAsyncTaskExecutor()) // concurrency
            .throttleLimit(4) // concurrency level
            .build())
        .build();
}

// Job execution example (e.g., in a @Component or main method):
@Autowired
private JobLauncher jobLauncher;
@Autowired
private Job etlJob;

public void runJob() throws Exception {
    JobParameters params = new JobParametersBuilder()
        .addLong("time", System.currentTimeMillis())
        .toJobParameters();
    JobExecution execution = jobLauncher.run(etlJob, params);
}
```
Requires multiple beans, configuration files, domain classes, a Spring context, explicit concurrency configuration, and job execution code.


## License

LGPL v3