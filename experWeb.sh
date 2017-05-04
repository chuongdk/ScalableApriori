echo "Delte target/MApriori-0.0.1-SNAPSHOT.jar..........................."
rm target/MApriori-0.0.1-SNAPSHOT.jar 
echo "Building .jar ............................................"
mvn package

#hdfs dfs -put ../data/webdocs.dat input


#echo "Beta 100% Support 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 338416  10000000  500


#echo "Beta 100% Support 15%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 253812 10000000  500




#echo "Beta 50% Support 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 338416  846041  500


#echo "Beta 50% Support 15%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 253812  846041  500



#echo "Beta 30% Support 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 338416  507625  500


#echo "Beta 30% Support 15%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 253812 507625  500










#echo "Beta 20% Support 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 338416  338416  500


echo "Beta 15% Support 15%"
hdfs dfs -rm -r /user/chuong/output
hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 253812 253812   500















#hdfs dfs -get output webdoc10

#echo "Beta 50%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 169208 846041 500


#echo "MApriori 9%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 152287 507625 500

#echo "DApriori 9% 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 152287 846041 500


#echo "MApriori 8%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 135367 507625 500

#echo "DApriori 8% 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 135367 846041 500

#echo "MApriori 7%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 118446 507625 500

#echo "DApriori 7% 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 118446 846041 500

#echo "MApriori 6%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 101525 507625 500

#echo "DApriori 6% 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 101525 846041  500


#echo "DApriori 5% 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 84604 507625  500

#echo "DApriori 5% 20%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 84604 846041  500

#echo "4%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 67683 111846041  500

#echo "3%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 50762 111846041  500

#echo "2%"
#hdfs dfs -rm -r /user/chuong/output
#hadoop jar target/MApriori-0.0.1-SNAPSHOT.jar  girafon.MApriori.App input/webdocs.dat output 33841 111846041  500



echo "DONE..................."

