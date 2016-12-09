/*
Databricks notebook source exported at Fri, 9 Dec 2016 00:51:02 UTC
Spark for Data Science Enriched Tutorial by various ML Models:
*/
//read the taxes datas
val taxes2013 = sqlContext
  .read.format("com.databricks.spark.csv")
  .option("header", "true")
  .load("dbfs:/databricks-datasets/data.gov/irs_zip_code_data/data-001/2013_soi_zipcode_agi.csv")


//read the market data
val markets = sqlContext
  .read.format("com.databricks.spark.csv")
  .option("header", "true")
  .load("dbfs:/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/market_data.csv")



// lets register the tbles to use sql.context
taxes2013.registerTempTable("taxes2013")
markets.registerTempTable("markets")



//display(markets)
display(taxes2013)

// In sql cell list the tables
// %sql show tables




// SQL cell !!!
// %sql SELECT N1,agi_stub FROM taxes2013
//-- lets see what is in our table



// %sql
// -- cleaning operation will be done
// DROP TABLE IF EXISTS cleaned_taxes;
//
// CREATE TABLE cleaned_taxes AS
// SELECT state, int(zipcode / 10) as zipcode,
//   int(mars1) as single_returns,
//   int(mars2) as joint_returns,
//   int(numdep) as numdep,
//   double(A02650) as total_income_amount,
//   double(A00300) as taxable_interest_amount,
//   double(a01000) as net_capital_gains,
//   double(a00900) as biz_net_income
// FROM taxes2013



// %sql select * from cleaned_taxes
// -- you can also plot the data into world or any other graph type. Be sure that
// -- you choose the state code for the key.



// %sql select state, avg(total_income_amount) from cleaned_taxes group by state
// --- or we can write this as follows if we convert the table cleaned_taxes into sqlContex and scala object as next



// now lets get some descriptive statistics we can use as follow
val cleanedTaxes = sqlContext.table("cleaned_taxes")
display(cleanedTaxes.groupBy("state").avg("total_income_amount"))




//also we might get general statistics like below
display(cleanedTaxes.describe())



// %sql
// -- Let's look at the set of zip codes with the lowest total capital gains and plot the results.
// select zipcode, sum(net_capital_gains) as cap_gains
// from cleaned_taxes
// where not (zipcode = 0000 OR zipcode = 9999)
// group by zipcode
// order by cap_gains desc
// limit 10;
//



// Could not make it work.

// or similarly we could register the table as sqlContex and run the query
//display(cleanedTaxes.describe())
//display(cleanedTaxes.filter("zipcode"!=0000).groupBy("zipcode").sum("net_capita//l_gains"))



// %sql
// SELECT zipcode,
//   SUM(biz_net_income) as business_net_income,
//   SUM(net_capital_gains) as capital_gains,
//   SUM(net_capital_gains) + SUM(biz_net_income) as capital_and_business_income
// FROM cleaned_taxes
//   WHERE NOT (zipcode = 0000 OR zipcode = 9999)
// GROUP BY zipcode
// ORDER BY capital_and_business_income DESC
// LIMIT 50



// %sql
// --let see how it goes
// EXPLAIN
//   SELECT zipcode,
//  SUM(biz_net_income) as net_income,
//  SUM(net_capital_gains) as cap_gains,
//  SUM(net_capital_gains) + SUM(biz_net_income) as combo
//   FROM cleaned_taxes
//   WHERE NOT (zipcode = 0000 OR zipcode = 9999)
//   GROUP BY zipcode
//   ORDER BY combo desc
//   limit 50

// equivalent to the above
sqlContext.sql("""
  SELECT zipcode,
    SUM(biz_net_income) as net_income,
    SUM(net_capital_gains) as cap_gains,
    SUM(net_capital_gains) + SUM(biz_net_income) as combo
  FROM cleaned_taxes
  WHERE NOT (zipcode = 0000 OR zipcode = 9999)
  GROUP BY zipcode
  ORDER BY combo desc
  limit 50""").explain



//Cachching the table
sqlContext.cacheTable("cleaned_taxes")

//or you can use the following in sql
//%sql CACHE TABLE cleaned_taxes


// %sql
// SELECT zipcode,
//   SUM(biz_net_income) as net_income,
//   SUM(net_capital_gains) as cap_gains,
//   SUM(net_capital_gains) + SUM(biz_net_income) as combo
// FROM cleaned_taxes
// WHERE NOT (zipcode = 0000 OR zipcode = 9999)
// GROUP BY zipcode
// ORDER BY combo desc
// limit 50
//
// -- run this twice to see the difference of in memory computation.


display(markets.groupBy("State").count())



/*
While these datasets probably warrant a lot more exploration, let's go ahead and prep the data for use in Apache Spark MLLib.
Apache Spark MLLib has some specific requirements about how inputs are structured.
Firstly input data has to be numeric unless you're performing a transformation inside of a data pipeline.
What this means for you as a user is that Apache Spark won't automatically convert string to categories for instance and the output will be a Double type.
Let's go ahead and prepare our data so that it meets those requirements as well as joining together our input data with the target variable - the number of farmer's markets in a given zipcode.
*/

val cleanedTaxes = sqlContext.sql("SELECT * FROM cleaned_taxes")
//cleanedTaxes.collect()

// sum up all the columns gruop by the zipcodes
val summedTaxes = cleanedTaxes
                  .groupBy("zipcode")
                  .sum()

// here selectExpresion is same as SQL SELEct expression
val cleanedMarkets = markets
                     .selectExpr("*","int(zip / 10) as zipcode")
                  .groupBy("zipcode")
                  .count()
                  .selectExpr("double(count) as count", "zipcode as zip")
//display(cleanedMarkets)

// joined = table_A.join(table_B,table_A(colx)=table_B(coly), "outer/inner/left join")
val joined = cleanedMarkets
             .join(summedTaxes,cleanedMarkets("zip")===summedTaxes("zipcode"),"outer")


//display(cleanedTaxes)
//display(cleanedMarkets)
display(joined)

// lets clean the null values in count and zip column. MLLib doesnt allow null values
val prepped = joined.na.fill(0)
display(prepped)

/*
Now that all of our data is prepped. We're going to have to put all of it into one column of a vector type for Spark MLLib. This makes it easy to embed a prediction right in a DataFrame and also makes it very clear as to what is getting passed into the model and what isn't without have to convert it to a numpy array or specify an R formula. This also makes it easy to incrementally add new features, simply by adding to the vector. In the below case rather than specifically adding them in, I'm going to create a exclusionary group and just remove what is NOT a feature.
*/
val nonFeatureCols = Array("zip","zipcode","count")
val featureCols = prepped.columns.diff(nonFeatureCols)
// both are array




/*
Now I'm going to use the VectorAssembler in Apache Spark to Assemble all of these columns into one single vector. To do this I'll have to set the input columns and output column. Then I'll use that assembler to transform the prepped data to my final dataset.
*/
import org.apache.spark.ml.feature.VectorAssembler
val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

//org.apache.spark.sql.DataFrame
val finalPrep = assembler.transform(prepped)



display(finalPrep.drop("zip").drop("zipcode").drop("features"))



// split the data set for training and testing each of them are //org.apache.spark.sql.DataFrame
val Array(training, test) = finalPrep.randomSplit(Array(0.7, 0.3))

//Lets record them into memory
training.cache()
test.cache()

println(training.count)
println(test.count)



 /*
Now Machine Learning Library is in action. Lets first create an instance of a regressor or classifier, that in turn will then be trained and return a Model type. Whenever you access Spark MLLib you should be sure to import/train on the name of the algorithm you want as opposed to the Model type.
For example: you should import:
org.apache.spark.ml.regression.LinearRegression
as opposed to:
org.apache.spark.ml.regression.LinearRegressionModel
*/

import org.apache.spark.ml.regression.LinearRegression

// get an instance of a Linear Regression class
var lrModel = new LinearRegression()
                  .setLabelCol("count")
                  .setFeaturesCol("features")
                  .setElasticNetParam(0.5)

// Lets see the configuration and parameter settings of
println("Printing out the model Parameters:")
println("-"*20)
//explain parametes
println(lrModel.explainParams)
println("-"*20)

import org.apache.spark.mllib.evaluation.RegressionMetrics
val lrFitted = lrModel.fit(training)

val holdout = lrFitted
  .transform(test)
  .selectExpr("prediction as raw_prediction",
    "double(round(prediction)) as prediction",
    "count",
    """CASE double(round(prediction)) = count
  WHEN true then 1
  ELSE 0
END as equal""")
display(holdout)


//display(holdout.selectExpr("sum(equal)"))
//holdout.selectExpr("sum(1)").collect()
//display(holdout.selectExpr("sum(equal)"))
//holdout.selectExpr("sum(1)").collect()
//display(holdout.describe())

//lets see the ratio of exact match
display(holdout.selectExpr("sum(equal)/sum(1)"))

// Evaluation by various metrics.But first need to do type conversion
val rm = new RegressionMetrics(
                              holdout
                              .select("prediction", "count") // select the prediction and actual count column
                              .rdd // rdd conversion for passing argument
                              .map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))//type conversion
                              )

println("MSE(Mean Square Error): " + rm.meanSquaredError)
println("MAE(Mean Absolute Error): " + rm.meanAbsoluteError)
println("RMSE(RootMeanSquaredError) Squared: " + rm.rootMeanSquaredError)
println("R Squared: " + rm.r2)
println("Explained Variance: " + rm.explainedVariance + "\n")



import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineStage}

/*
Lets try another model
*/

// Create the instance of RandomForest Regression
val rfModel = new RandomForestRegressor()
  .setLabelCol("count")
  .setFeaturesCol("features")

// Lets create instance of paramater builder and grids for optimum result.
val paramGrid = new ParamGridBuilder()
  .addGrid(rfModel.maxDepth, Array(5))
  .addGrid(rfModel.numTrees, Array(30, 60))
  .build()



val steps:Array[PipelineStage] = Array(rfModel)
val pipeline = new Pipeline().setStages(steps)

val cv = new CrossValidator() // feel free to change the number of folds used in cross validation as well
  .setEstimator(pipeline) // the estimator can also just be an individual model rather than a pipeline
  .setEstimatorParamMaps(paramGrid)
  .setEvaluator(new RegressionEvaluator().setLabelCol("count"))

val pipelineFitted = cv.fit(training)


// Lets look at the which grid is the best option
println("The Best Parameters:\n--------------------")
println(pipelineFitted.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
pipelineFitted
  .bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
  .stages(0)
  .extractParamMap


//result data frame
val holdout2 = pipelineFitted.bestModel
  .transform(test)
  .selectExpr("prediction as raw_prediction",
    "double(round(prediction)) as prediction",
    "count",
    """CASE double(round(prediction)) = count
  WHEN true then 1
  ELSE 0
END as equal""")
// Lets display the result
display(holdout2)


val rm2 = new RegressionMetrics(
  holdout2.select("prediction", "count").rdd.map(x =>
  (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

println("MSE: " + rm2.meanSquaredError)
println("MAE: " + rm2.meanAbsoluteError)
println("RMSE Squared: " + rm2.rootMeanSquaredError)
println("R Squared: " + rm2.r2)
println("Explained Variance: " + rm2.explainedVariance + "\n")

//Lets look at the ratio of exact match to total
display(holdout2.selectExpr("sum(equal)/sum(1)"))
