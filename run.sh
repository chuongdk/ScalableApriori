echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/*.jar 
echo "Building .jar ............................................"
mvn package
echo "Running .................................................."
hdfs dfs -rm -r /user/chuong/output
hadoop jar target/ScalableApriori-0.0.1-SNAPSHOT.jar  girafon.ScalableApriori.App input/retail.dat output 100 1 1
echo "DONE"
wc -l result_*
