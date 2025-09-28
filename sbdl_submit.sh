spark-submit --master yarn --deploy-mode cluster \
--py-files sdbl_lib.zip \
--files conf/sdbl.conf,conf/spark.conf,log4j.properties \
--driver-cores 2 \
--driver-memory 3G \
--conf spark.driver.memoryOverhead=1G
main.py qa 2025-09-23