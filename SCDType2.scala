
import org.apache.spark.sql.SparkSession
import spark.implicits._

val spark = SparkSession.
  builder.
  appName("Slow Changing Dimentions Type 2 Demo").
  getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", true)


// create a customer DF with original data

case class Customer(pk: Int, amount: Float, StartDate: String, endDate: String, active: Int)

val CustomerDF = Seq(
  Customer(1, 10, "2019-01-01 12:00:00", "2019-01-20 05:00:00", 0),
  Customer(1, 20, "2019-01-20 05:00:00", null,1),
  Customer(2, 100, "2019-01-01 00:00:00", null, 1),
  Customer(3, 75, "2019-01-01 06:00:00", "2019-01-26 08:00:00", 0),
  Customer(3, 750, "2019-01-26 08:00:00", null, 1),
  Customer(10, 40, "2019-01-01 00:00:00", null, 1)

).toDF()

// write the customer data as dataframe to local or hdfs

CustomerDF.write.format("delta").mode("overwrite").save("/user/original_data")


val original_data = spark.read.format("delta").load("/user/original_data")


// create a customer updated data, new incoming data

case class CustomerUpdate(pk: Int, amount: Float, StartDate: String, endDate: String, active: Int)

val SourceDF = Seq(
  CustomerUpdate(1, 50, "2019-02-01 07:00:00", "2019-02-02 08:00:00", 0),
  CustomerUpdate(1, 75, "2019-02-02 08:00:00", null,1),
  CustomerUpdate(2, 200, "2019-02-01 05:00:00", "2019-02-01 13:00:00", 0),
  CustomerUpdate(2, 60, "2019-02-01 13:00:00", "2019-02-01 19:00:00", 0),
  CustomerUpdate(2, 500, "2019-02-01 19:00:00", null, 1),
  CustomerUpdate(3, 175, "2019-02-01 00:00:00", null, 1),
  CustomerUpdate(4, 50, "2019-02-02 12:00:00", "2019-02-02 14:00:00", 0),
  CustomerUpdate(4, 300, "2019-02-02 14:00:00", null, 1),
  CustomerUpdate(5, 500, "2019-02-02 00:00:00", null, 1)
).toDF()


// write the customer data as dataframe to local or hdfs

SourceDF.write.format("delta").mode("overwrite").save("/user/source_table")

val updatedtable = spark.read.format("delta").load("/user/source_table")

// Find the new records from incoming data table

val newRecordsToUpdate = updatedtable.as("updates").
  join(original_data.as("customers"), "pk").
  where("customers.active = 1 AND updates.active = 1 AND updates.amount <> customers.amount")

// create a mergeKey where new records will have null values as well as their pk as their mergeKey,
// this step is important to find matching rows in the existing table, so we update the endData & status
// also, null valued records will be inserted to the existing table as new records

val stagedUpdates = newRecordsToUpdate.
  selectExpr("NULL as mergeKey", "updates.*").
  union(updatedtable.as("updates").filter(updatedtable("active") ===1).
    selectExpr("updates.pk as mergeKey", "*"))

// this records are old records with the incoming table, we will union with delta table
val oldUpdates = updatedtable.as("updates").
  where("updates.active = 0").
  select("updates.*")


// import delta tables and read the original table as delta table

import io.delta.tables._


val existingtable2 = DeltaTable.forPath(spark, "/user/original_data")


// using merge to update records that are expiring and insert new records to existing table

existingtable2.as("customers").
  merge(stagedUpdates.as("staged_updates"),"customers.pk = staged_updates.mergeKey").
  whenMatched("customers.active = 1 AND customers.amount <> staged_updates.amount").
  updateExpr(Map(
      "active" -> "0",
      "endDate" -> "staged_updates.StartDate")).
  whenNotMatched().
  insertExpr(
    Map("pk" -> "staged_updates.pk",
      "amount" -> "staged_updates.amount",
      "StartDate" -> "staged_updates.StartDate",
      "endDate" -> "staged_updates.endDate",
      "active" -> "1")).
  execute()

// union the delta table original_data with the oldupdate dataframe created in previous step.

DeltaTable.
  forPath(spark, "/user/original_data").
  toDF.union(oldUpdates).
  orderBy("pk", "amount").show()
