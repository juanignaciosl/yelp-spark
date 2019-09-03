# Yelp processing with Spark

This is a playground for me to fiddle with Spark.

It uses [a Yelp sample dataset](https://www.yelp.com/dataset/) ([documentation](https://www.yelp.com/dataset/documentation/main)).

## Roadmap

- [X] Happy path of an analysis of open businesses, median and p95 opening and closing times.
- [X] Happy path pieces testing.
- [X] Local run.
- [X] Docker cluster run.
- [ ] Make code failures more observable. Example: add more wrong data and check that you can see the broken line.
- [ ] Review and optimize performance.
- [ ] Mock x10 data and check that aggregation scales properly.
- [ ] Better output formats:
  - [ ] SQLite for better typing and interoperability.
  - [ ] Parquet or Avro for performance.
- Pending fixes:
  - [ ] Handle "coolness ties" in a deterministic way.
- [ ] Refactors
    - [ ] Can we take advantage of first grouping by state and city, and then by postal code?
    - [ ] Split code into different files.
    - [ ] Use a `Map` instead of a `WeekHours` case class to make code simpler.
    - [ ] Use integer minutes instead of strings for time parsing for simplicity and performance reasons.
    - [ ] Redo towards a pure Dataset, SQL solution and compare performance.
    - [ ] Simplify and unify CSV output.
    - [ ] Go back to Scala 2.12.8 (had to downgrade to 2.11.8 for compatibility with Spark libraries).

## Components

There are two runners, `YelpBusinessesLocalRunner` for local purposes and `YelpBusinessesRunner`, for `spark-submit`.

Both extend `YelpDataProcessor`, which calls the business logic at `YelpBusinesses` and outputs the result to CSV files.

`YelpBusinesses` contains all the logic in public methods for grouping, counting, etc.
Most of the work is actually performed by three `Aggregator`s:
`PercentileAggregator`, `CountOpenPastTimeAggregator` and `CoolestBusinessAggregator`.
`Aggregator`s are the optimal way to apply `agg` to grouped datasets in Spark SQL so the
shuffling is minimized. That should make this solution scalable.

## Running

### Locally

Run `com.juanignaciosl.yelp.YelpBusinessesLocalRunner` with the path containing `business.json` and `review.json` as the first argument.
The second argument is the output folder for the files, `/tmp` by default

### Docker

Note: [source](https://github.com/mvillarrealb/docker-spark-cluster).

```bash
docker build -t spark-submit:2.3.1 ./docker/spark-submit
sbt clean package
cp target/scala-2.11/yelpspark_2.11-0.1.0-SNAPSHOT.jar docker-volumes/apps/
cp <path-to-yelp-data>/business.json docker-volumes/data/
cp <path-to-yelp-data>/review.json docker-volumes/data/
docker-compose up --scale spark-worker=3
```

In order to submit the jobs:

```bash
./submit.sh
```

#### Useful commands for debugging purposes

- `docker exec -ti yelp-spark_spark-worker_3 ls -lt /spark/work`: list driver traces at a worker.
- `docker exec -ti yelp-spark_spark-worker_3 cat /spark/work/driver-20190831075201-0000/stderr`: show output.
