FROM spydernaz/spark-base:latest

COPY spark-submit.sh /

ENV SPARK_MASTER_URL="spark://spark-master:7077"



CMD ["/bin/bash", "/spark-submit.sh"]
