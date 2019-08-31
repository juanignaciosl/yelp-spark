#  --env SPARK_APPLICATION_JAR_LOCATION=/opt/spark-apps/YelpSpark-assembly-0.1.0-SNAPSHOT.jar \
#  --env SPARK_APPLICATION_JAR_LOCATION=/opt/spark-apps/yelpspark_2.12-0.1.0-SNAPSHOT.jar \
sudo docker run --network yelp-spark_default \
  --env SPARK_APPLICATION_JAR_LOCATION=/opt/spark-apps/yelpspark_2.11-0.1.0-SNAPSHOT.jar \
  --env SPARK_APPLICATION_MAIN_CLASS="com.juanignaciosl.yelp.YelpBusinessesRunner" \
  --env SPARK_APPLICATION_ARGS="/opt/spark-data /opt/spark-output" \
  --env SPARK_WORKER_LOG_DIR="/opt/spark-logs" \
  -v $PWD/docker-volumes/apps:/opt/spark-apps \
  spark-submit:2.3.1
