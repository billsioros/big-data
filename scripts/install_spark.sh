#!/bin/bash
SPARK_VERSION=spark-3.5.1
USER=osboxes

download_spark () {
	cd ~
	wget https://dlcdn.apache.org/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz --no-check-certificate
	tar -xzf $SPARK_VERSION-bin-hadoop3.tgz

}

configure_spark () {

	echo "export SPARK_HOME=/home/$USER/$SPARK_VERSION-bin-hadoop3" >> ~/.bashrc
	echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
	echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
	echo "alias start-all.sh='\$SPARK_HOME/sbin/start-all.sh'" >> ~/.bashrc
	echo "alias stop-all.sh='\$SPARK_HOME/sbin/stop-all.sh'" >> ~/.bashrc

	source ~/.bashrc

	cd /home/$USER/$SPARK_VERSION-bin-hadoop3/conf

	cp spark-env.sh.template spark-env.sh
	echo "SPARK_WORKER_CORES=2" >> spark-env.sh
	echo "SPARK_WORKER_MEMORY=3g" >> spark-env.sh

	cp spark-defaults.conf.template spark-defaults.conf
	echo "spark.master		spark://master:7077" >> spark-defaults.conf
	echo "spark.submit.deployMode		client" >> spark-defaults.conf
	echo "spark.executor.instances		2" >> spark-defaults.conf
	echo "spark.executor.cores		2" >> spark-defaults.conf
	echo "spark.executor.memory		3g" >> spark-defaults.conf
	echo "spark.driver.memory		512m" >> spark-defaults.conf

	echo "master" > slaves
	echo "slave" >> slaves
}


echo "STARTING DOWNLOAD ON MASTER"
download_spark

echo "STARTING DOWNLOAD ON SLAVE"
ssh $USER@slave "$(typeset -f download_spark); SPARK_VERSION=$SPARK_VERSION; USER=$USER; download_spark"

echo "STARTING HADOOP CONFIGURE ON MASTER"
configure_spark

echo "STARTING HADOOP CONFIGURE ON SLAVE"
ssh $USER@slave "$(typeset -f configure_spark); SPARK_VERSION=$SPARK_VERSION; USER=$USER; configure_spark"
