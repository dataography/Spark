
sc
. \t   // type dot and tab

// first object is to parallelize, second parameter determines the number of partion
val rdd  = sc.parallelize(Array(1,2,2,4),4)

val rawblocks = sc.textFile("linkage")

rawblocks.first()
//res22: String = "id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"

val head = rawblocks.take(10)
//head: Array[String] = Array("id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match", 37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE, 39086,47614,1,?,1,?,1,1,1,1,1,TRUE, 70031,70237,1,?,1,?,1,1,1,1,1,TRUE, 84795,97439,1,?,1,?,1,1,1,1,1,TRUE, 36950,42116,1,?,1,1,1,1,1,1,1,TRUE, 42413,48491,1,?,1,?,1,1,1,1,1,TRUE, 25965,64753,1,?,1,?,1,1,1,1,1,TRUE, 49451,90407,1,?,1,?,1,1,1,1,0,TRUE, 39932,40902,1,?,1,?,1,1,1,1,1,TRUE)



head.length
// res24: Int = 10

//return number of objects in RDD
rdd.count
//res25: Long = 4


rdd.collect()
//res26: Array[Int] = Array(1, 2, 2, 4)

head.foreach(println)
/*
"id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"
37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE
39086,47614,1,?,1,?,1,1,1,1,1,TRUE
70031,70237,1,?,1,?,1,1,1,1,1,TRUE
84795,97439,1,?,1,?,1,1,1,1,1,TRUE
36950,42116,1,?,1,1,1,1,1,1,1,TRUE
42413,48491,1,?,1,?,1,1,1,1,1,TRUE
25965,64753,1,?,1,?,1,1,1,1,1,TRUE
49451,90407,1,?,1,?,1,1,1,1,0,TRUE
39932,40902,1,?,1,?,1,1,1,1,1,TRUE
*/

def isHeader(line:String):Boolean = {
      line.contains("id_1")}


//head.collect() not working ??
head.filter(isHeader).foreach(println)
//"id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"


// yet following is notworking
head.filter(!isHeader).foreach(println)

// on the other Hand the following are working
head.filter(!isHeader(_)).foreach(println)
head.filter(x => !isHeader(x)).foreach(println)
head.filterNot(isHeader).foreach(println)

val noheader = rawblocks.filter(!isHeader(_))


//fifth element of head
val line = head(5)
//line: String = 36950,42116,1,?,1,1,1,1,1,1,1,TRUE
val pieces = line.split(",")

val id1 = pieces(0).toInt
val id2 = pieces(1).toInt

val matched = pieces(11).toBoolean


//  TYPES  IMPORTANTE

/**
 pieces
res3: Array[String] = Array(36950, 42116, 1, ?, 1, 1, 1, 1, 1, 1, 1, TRUE)

 line
res4: String = 36950,42116,1,?,1,1,1,1,1,1,1,TRUE

 head
res5: Array[String] = Array("id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match", 37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE, 39086,47614,1,?,1,?,1,1,1,1,1,TRUE, 70031,70237,1,?,1,?,1,1,1,1,1,TRUE, 84795,97439,1,?,1,?,1,1,1,1,1,TRUE, 36950,42116,1,?,1,1,1,1,1,1,1,TRUE, 42413,48491,1,?,1,?,1,1,1,1,1,TRUE, 25965,64753,1,?,1,?,1,1,1,1,1,TRUE, 49451,90407,1,?,1,?,1,1,1,1,0,TRUE, 39932,40902,1,?,1,?,1,1,1,1,1,TRUE)

 rawblocks
res6: org.apache.spark.rdd.RDD[String] = linkage MapPartitionsRDD[6] at textFile at <console>:24

 val array_of_string_out_og_rdd = rawblocks.take(10)
array_of_string_out_og_rdd: Array[String] = Array("id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match", 37291,53113,0.833333333333333,?,1,?,1,1,1,1,0,TRUE, 39086,47614,1,?,1,?,1,1,1,1,1,TRUE, 70031,70237,1,?,1,?,1,1,1,1,1,TRUE, 84795,97439,1,?,1,?,1,1,1,1,1,TRUE, 36950,42116,1,?,1,1,1,1,1,1,1,TRUE, 42413,48491,1,?,1,?,1,1,1,1,1,TRUE, 25965,64753,1,?,1,?,1,1,1,1,1,TRUE, 49451,90407,1,?,1,?,1,1,1,1,0,TRUE, 39932,40902,1,?,1,?,1,1,1,1,1,TRUE)

 val slice_string_out_of_rdd = array_of_string_out_og_rdd(3)
slice_string_out_of_rdd: String = 70031,70237,1,?,1,?,1,1,1,1,1,TRUE

 val slice_string_to_array_string = slice_string_out_of_rdd.toArray
slice_string_to_array_string: Array[Char] = Array(7, 0, 0, 3, 1, ,, 7, 0, 2, 3, 7, ,, 1, ,, ?, ,, 1, ,, ?, ,, 1, ,, 1, ,, 1, ,, 1, ,, 1, ,, T, R, U, E)

 val slice_string_to_array_string1 = slice_string_out_of_rdd.split(",")
slice_string_to_array_string1: Array[String] = Array(70031, 70237, 1, ?, 1, ?, 1, 1, 1, 1, 1, TRUE)

*/

val rawscores = pieces.slice(2,11)
//rawscores: Array[String] = Array(1, ?, 1, 1, 1, 1, 1, 1, 1)

val rawscores = pieces.slice(0,11)
//rawscores: Array[String] = Array(36950, 42116, 1, ?, 1, 1, 1, 1, 1, 1, 1)

rawscores.map(s => s.toDouble)
//wont work since there is ? character which cant be converted into a number

// To handle this kind of scenario  lets write a function

def toDouble(s:String)= {
  if ("?".equals(s)) Double.NaN else s.toDouble
}

val scores = rawscores.map(toDouble)
//scores: Array[Double] = Array(1.0, NaN, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

def parse(line: String) = {
  val pieces = line.split(",")
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val matched = pieces(11).toBoolean
  val scores = pieces.slice(2,11).map(toDouble)
  (id1,id2,scores,matched)
}

 val tup = parse(line)
//tup: (Int, Int, Array[Double], Boolean) = (36950,42116,Array(1.0, NaN, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),true)

 tup._1
//res29: Int = 36950

 tup._3
//res30: Array[Double] = Array(1.0, NaN, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

 tup._3(0)
//res31: Double = 1.0

 tup._3(1)
//res32: Double = NaN

 tup._3.length
//res33: Int = 9

//size of a tuple
tup.productArity
//res34: Int = 4

// Or you can get element by productElement method starting by 0
tup.productElement(0)
res35: Any = 36950

 tup.productElement(1)
res36: Any = 42116

 tup.productElement(3)
res37: Any = true



case class MatchData(id1: Int, id2: Int, scores:Array[Double],matched:Boolean)

def parse(line: String) = {
  val pieces = line.split(",")
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val matched = pieces(11).toBoolean
  val scores = pieces.slice(2,11).map(toDouble)
  MatchData(id1,id2,scores,matched)
}

val md = parse(line)
//md: MatchData = MatchData(36950,42116,[D@4939e1d8,true)

(md.id1,md.id2,md.matched)
//res40: (Int, Int, Boolean) = (36950,42116,true)

md.id1
//res41: Int = 36950
//now we can call items as a attribute of the class


val mds = head.filter(x => !isHeader(x)).map(x => parse(x))
//or
val mds = head.filter(!isHeader(_)).map(parse(_))
//mds: Array[MatchData] = Array(MatchData(37291,53113,[D@164afa6f,true), MatchData(39086,47614,[D@1a66d86b,true), MatchData(70031,70237,[D@18d28cf6,true), MatchData(84795,97439,[D@5eb634c8,true), MatchData(36950,42116,[D@1fee34f3,true), MatchData(42413,48491,[D@33e712e5,true), MatchData(25965,64753,[D@3fde8d53,true), MatchData(49451,90407,[D@50a1ee14,true), MatchData(39932,40902,[D@3e86015f,true))

//Now lets apply our parsing function to the data in the cluster by calling the map function to whole data
val parsed = noheader.map(line => parse(line))


// now lets update our parse method to return as case class

// AGGREGATION
// groupBy created an object: [Boolean,Array[MatchData]]
// or val grouped = mds.groupBy(_.matched)
val grouped = mds.groupBy(md => md.matched)

grouped.mapValues(x => x.length).foreach(println)
//(true,9)

val matchCounts = parsed.map(_.matched).countByValue()
matchCounts: scala.collection.Map[Boolean,Long] = Map(true -> 20931, false -> 5728201)

val matchCounts = parsed.map(_.matched).countByValue().foreach(println)
(true,20931)
(false,5728201)

// scala object dont gives sorting property for maps key nad or values. Does
// convert it into Scala Seq type. which provides sorting.
val matchCountsSeq = matchCounts.toSeq

matchCountsSeq.sortBy(_._1).foreach(println)
//(false,5728201)
//(true,20931)

matchCountsSeq.sortBy(_._2).foreach(println)
//(true,20931)
//(false,5728201)

matchCountsSeq.sortBy(_._2).reverse.foreach(println)
//(false,5728201)
//(true,20931)

parsed.map(md => md.scores(0)).stats
//res9: org.apache.spark.util.StatCounter = (count: 5749132, mean: NaN, stdev: NaN, max: NaN, min: NaN)
import java.lang.Double.isNaN

parsed.map(md => md.scores(0)).filter(!isNaN(_)).take(3)
//res13: Array[Double] = Array(0.833333333333333, 1.0, 1.0)
parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()
//res15: org.apache.spark.util.StatCounter = (count: 5748125, mean: 0.712902, stdev: 0.388758, max: 1.000000, min: 0.000000)


val stats = (0 until 9).map(i =>{
  parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
})

stats.foreach(println)
/*
(count: 5748125, mean: 0.712902, stdev: 0.388758, max: 1.000000, min: 0.000000)
(count: 103698, mean: 0.900018, stdev: 0.271316, max: 1.000000, min: 0.000000)
(count: 5749132, mean: 0.315628, stdev: 0.334234, max: 1.000000, min: 0.000000)
(count: 2464, mean: 0.318413, stdev: 0.368492, max: 1.000000, min: 0.000000)
(count: 5749132, mean: 0.955001, stdev: 0.207301, max: 1.000000, min: 0.000000)
(count: 5748337, mean: 0.224465, stdev: 0.417230, max: 1.000000, min: 0.000000)
(count: 5748337, mean: 0.488855, stdev: 0.499876, max: 1.000000, min: 0.000000)
(count: 5748337, mean: 0.222749, stdev: 0.416091, max: 1.000000, min: 0.000000)
(count: 5736289, mean: 0.005529, stdev: 0.074149, max: 1.000000, min: 0.000000)
*/
stats(1)
//res17: org.apache.spark.util.StatCounter = (count: 103698, mean: 0.900018, stdev: 0.271316, max: 1.000000, min: 0.000000)
