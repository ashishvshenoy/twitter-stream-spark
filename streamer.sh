hadoop fs -rmr /user/ubuntu/monitoring2/*
hadoop fs -cp /user/ubuntu/split-dataset2/* /user/ubuntu/split-dataset
for i in {1..1127}
do
	hadoop fs -mv /user/ubuntu/split-dataset/${i}.csv /user/ubuntu/monitoring2/
	sleep 5
done
hadoop fs -rmr /user/ubuntu/monitoring2/*
hadoop fs -cp /user/ubuntu/split-dataset2/* /user/ubuntu/split-dataset
