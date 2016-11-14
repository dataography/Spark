//put the data in hdfs file system
# hdfs dfs -put /user/ds/user_artist_data.txt

//read the data
 val rawUserArtistData = sc.textFile("hdfs:///user/ds/user_artist_data.txt")


 rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
//res5: org.apache.spark.util.StatCounter = (count: 24296858, mean: 1947573.265353, stdev: 496000.544975, max: 2443548.000000, min: 90.000000)

rawUserArtistData.map(_.split(' ')(1).toDouble).stats()
//res6: org.apache.spark.util.StatCounter = (count: 24296858, mean: 1718704.093757, stdev: 2539389.040171, max: 10794401.000000, min: 1.000000)

rawUserArtistData.map(_.split(' ')(2).toDouble).stats()
//res7: org.apache.spark.util.StatCounter = (count: 24296858, mean: 15.295762, stdev: 153.915321, max: 439771.000000, min: 1.000000)
