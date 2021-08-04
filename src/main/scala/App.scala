import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

object App {

  var spark:SparkSession = null

  def main(args: Array[String]): Unit = {
    spark = spark_session_init()
    println("-- Created spark session --")
    spark.sparkContext.setLogLevel("ERROR") //Remove some of the logging from the spark Session logger
    //spark_test()

    //remove_all_data() //Used to reset the base tables
    //create_base_tables()
    //load_base_data()

    //problem_scenario_1()
    problem_scenario_2()
    problem_scenario_3()
    problem_scenario_4()
    problem_scenario_5()
    problem_scenario_6()
  }

  def problem_scenario_1(): Unit = {
    group_similar_tables()

    //spark.sql("drop view bev_branch1")
    //spark.sql("create view bev_branch1 as select distinct bev_type, branch_num from bev_branch_a where branch_num = \"Branch1\"")
    //spark.sql("select * from bev_branch1").show()

    spark.sql("drop view bev_branch1_cons")
    spark.sql("create view bev_branch1_cons as " +
      "select bev.bev_type, bev.branch_num, con.consumer_count " +
      "from bev_branch1 bev inner join bev_conscount_total con " +
      "on bev.bev_type = con.bev_type")

    //spark.sql("select * from bev_branch1_cons").show()

    println("Problem Scenario 1:")
    println("The total number of consumers for Branch 1 is: ")
    spark.sql("select sum(consumer_count) as total_branch_1_consumers from bev_branch1_cons").show()

  }
  def problem_scenario_2(): Unit = {

  }
  def problem_scenario_3(): Unit = {

  }
  def problem_scenario_4(): Unit = {

  }
  def problem_scenario_5(): Unit = {

  }
  def problem_scenario_6(): Unit = {

  }

  /* Setup Spark and Test */

  def spark_session_init(): SparkSession = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    return SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
  }

  def spark_test(): Unit = {
    drop_table("newone")
    create_table("newone(id Int,name String)")
    load_local_table_data("newone","test.txt")
    show_all_data("newone")
  }

  /* Base table functions */

  def create_base_tables():Unit = {
    create_table("bev_branch_a(bev_type VARCHAR(255), branch_num VARCHAR(255))")
    create_table("bev_branch_b(bev_type VARCHAR(255), branch_num VARCHAR(255))")
    create_table("bev_branch_c(bev_type VARCHAR(255), branch_num VARCHAR(255))")

    //Conscount is the consumer count

    create_table("bev_conscount_a(bev_type VARCHAR(255), consumer_count INT)")
    create_table("bev_conscount_b(bev_type VARCHAR(255), consumer_count INT)")
    create_table("bev_conscount_c(bev_type VARCHAR(255), consumer_count INT)")
  }

  def load_base_data(): Unit = {
    load_local_table_data("bev_branch_a","Bev_BranchA.txt")
    load_local_table_data("bev_branch_b","Bev_BranchB.txt")
    load_local_table_data("bev_branch_c","Bev_BranchC.txt")
    load_local_table_data("bev_conscount_a","Bev_ConscountA.txt")
    load_local_table_data("bev_conscount_b","Bev_ConscountB.txt")
    load_local_table_data("bev_conscount_c","Bev_ConscountC.txt")
  }

  def remove_all_data(): Unit = {
    drop_table("bev_branch_a")
    drop_table("bev_branch_b")
    drop_table("bev_branch_c")
    drop_table("bev_conscount_a")
    drop_table("bev_conscount_b")
    drop_table("bev_conscount_c")
  }

  /* Problem Scenario supporting functions */

  def group_similar_tables(): Unit = {
    spark.sql("select count(bev_type) as A_total_rows from bev_branch_a").show()
    spark.sql("select count(bev_type) as B_total_rows from bev_branch_b").show()
    spark.sql("select count(bev_type) as C_total_rows from bev_branch_c").show()
    //spark.sql("drop view bev_conscount_total")
    spark.sql("create view bev_branch_full as " +
      "select * from bev_branch_a union all " +
      "select * from bev_branch_b union all " +
      "select * from bev_branch_c");
    //spark.sql("select * from bev_conscount_total limit 20").show()
    spark.sql("select count(bev_type) as Total_Rows from bev_branch_full").show()

    //    spark.sql("select count(consumer_count) as A_total_rows from bev_conscount_a").show()
    //    spark.sql("select count(consumer_count) as B_total_rows from bev_conscount_b").show()
    //    spark.sql("select count(consumer_count) as C_total_rows from bev_conscount_c").show()
    //    spark.sql("drop view bev_conscount_total")
    //    spark.sql("create view bev_conscount_total as " +
    //      "select * from bev_conscount_a union all " +
    //      "select * from bev_conscount_b union all " +
    //      "select * from bev_conscount_c");
    //    spark.sql("select * from bev_conscount_total limit 20").show()
    //    spark.sql("select count(consumer_count) as Total_Rows from bev_conscount_total").show()
  }

  /* Methods to reduce redundancy and handle certain exceptions */

  //Create a new table if it doesn't exist
  def create_table(table_signature: String): Unit = {
    spark.sql("create table if not exists " + table_signature  + "row format delimited fields terminated by ','")
    println("Table: " + table_signature + " was created successfully.")
  }

  //Create a new view if it doesn't exist
  def create_view(statement:String): Unit = {
    try {
      spark.sql(statement)
    }
    catch {
      case e: AnalysisException => println("Cannot Create View: One of the tables does not exist.")
    }
  }

  //Load local text file data from the input folder
  def load_local_table_data(table:String, textFileName:String): Unit = {
    try {
      spark.sql("LOAD DATA LOCAL INPATH 'input/" + textFileName + "' INTO TABLE " + table)
      println("Data: " + textFileName + " was successfully loaded into " + table + ".")
    }
    catch {
      case e: NoSuchTableException => println("Cannot Load Data: " + table + " does not exist.")
      case f: AnalysisException => println("Cannot Load Data: " + textFileName + " does not exist.")
    }
  }

  //Show all of the entries in the list (may be reduced by spark if output is too large)
  def show_all_data(name:String): Unit = {
    try {
      spark.sql("SELECT * FROM " + name).show()
    }
    catch {
      case e: AnalysisException => println("Cannot Show Data: " + name + " does not exist.")
    }
  }

  //Drop the table if it exists
  def drop_table(table:String): Unit = {
    try {
      spark.sql("drop table " + table)
      println("Table: " + table + " was dropped successfully.")
    }
    catch {
      case e: AnalysisException => println("Cannot Drop Table: " + table + " does not exist.")
    }
  }

  //Drop the view if it exists
  def drop_view(view:String): Unit = {
    try {
      spark.sql("drop view " + view)
      println("View: " + view + " was dropped successfully.")
    }
    catch {
      case e: AnalysisException => println("Cannot Drop View: " + view + " does not exist.")
    }
  }

}