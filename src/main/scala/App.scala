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

    //app_init() //Run to generate the appropriate base tables and views (Run only once if issues arise with data)

//    problem_scenario_1()
//    problem_scenario_2()
//    problem_scenario_3()
//    problem_scenario_4()
    problem_scenario_5()
//    problem_scenario_6()
  }

  /* Problem Scenario Functions */

  def prob_scen_1_setup(): Unit = {
    drop_table("bev_branch1")
    spark.sql("create table if not exists bev_branch1 as select distinct bev_type, branch_num from bev_branch_a where branch_num = \"Branch1\"")
    show_all_data("bev_branch1")

    drop_table("bev_branch1_cons")
    spark.sql("create table if not exists bev_branch1_cons as " +
      "select bev.bev_type, bev.branch_num, con.consumer_count " +
      "from bev_branch1 bev inner join bev_conscount_total con " +
      "on bev.bev_type = con.bev_type")

    show_all_data("bev_branch1_cons")

    // Branch 2

    drop_table("bev_branch2")
    spark.sql("create table if not exists bev_branch2 as " +
      "select distinct bev_type, branch_num from bev_branch_a where branch_num = \"Branch2\" union all " +
      "select distinct bev_type, branch_num from bev_branch_c where branch_num = \"Branch2\"")
    show_all_data("bev_branch2")

    drop_table("bev_branch2_cons")
    spark.sql("create table if not exists bev_branch2_cons as " +
      "select bev.bev_type, bev.branch_num, con.consumer_count " +
      "from bev_branch2 bev inner join bev_conscount_total con " +
      "on bev.bev_type = con.bev_type")

    show_all_data("bev_branch2_cons")
  }

  def problem_scenario_1(): Unit = {
    //prob_scen_1_setup()
    println("Problem Scenario 1:")
    println("The total number of consumers for Branch 1 is: ")
    spark.sql("select sum(consumer_count) as total_branch_1_consumers from bev_branch1_cons").show()

    println("The total number of consumers for Branch 2 is: ")
    spark.sql("select sum(consumer_count) as total_branch_2_consumers from bev_branch2_cons").show()
  }

  def problem_scenario_2(): Unit = {
    println("Problem Scenario 2:")
    println("The most consumed beverage(s) on branch 1 is:")
    spark.sql("select bev_type, consumer_count from " +
      "(select bev_type, consumer_count, rank() over (order by consumer_count desc) cid from bev_branch1_cons) " +
      "as v1 where cid = 1").show()

    println("The least consumed beverage(s) on branch 2 are:")
    spark.sql("select bev_type, consumer_count from " +
      "(select bev_type, consumer_count, rank() over (order by consumer_count asc) cid from bev_branch2_cons) " +
      "as v1 where cid = 1").show()

    println("The average consumed number of beverages on branch 2 is:")
    spark.sql("select round(avg(consumer_count), 2) as avg_num_of_bevs from bev_branch2_cons").show()
  }

  def prob_scen_3_setup(): Unit = {
    drop_table("bev_branch8")
    spark.sql("create table if not exists bev_branch8 as select distinct bev_type, branch_num from bev_branch_b where branch_num = \"Branch8\"")

    drop_table("bev_branch10") //Doesn't actually exist
    spark.sql("create table if not exists bev_branch10 as select distinct bev_type, branch_num from bev_branch_a where branch_num = \"Branch10\"")

    drop_table("bev_branch4")
    spark.sql("create table if not exists bev_branch4 as select distinct bev_type, branch_num from bev_branch_full where branch_num = \"Branch4\"")
    show_all_data("bev_branch4")

    drop_table("bev_branch7")
    spark.sql("create table if not exists bev_branch7 as select distinct bev_type, branch_num from bev_branch_full where branch_num = \"Branch7\"")
    show_all_data("bev_branch7")

    drop_table("bev_branch4_7")
    spark.sql("create table if not exists bev_branch4_7 as (" +
      "select distinct b4.bev_type " +
      "from bev_branch4 b4 inner join bev_branch7 b7 " +
      "where b4.bev_type = b7.bev_type)")

  }

  def problem_scenario_3(): Unit = {
    //prob_scen_3_setup()
    println("Problem Scenario 3:")
    println("The beverages available on branch 1 are:")
    show_all_data("bev_branch1")

    println("The beverages available on branch 8 are:")
    show_all_data("bev_branch8")

    println("The beverages available on branch 10 are:")
    show_all_data("bev_branch10")

    println("The beverages that are on both branch 4 and 7 are:")
    show_all_data("bev_branch4_7")

  }

  def prob_scen_4_setup(): Unit = {
    drop_table("bev_branch_full")
    spark.sql("create table if not exists bev_branch_full as " +
      "select * from bev_branch_a union all " +
      "select * from bev_branch_b union all " +
      "select * from bev_branch_c")

    drop_table("partit_bev_br")
    spark.sql("create table partit_bev_br (bev_type VARCHAR(255)) partitioned by (branch VARCHAR(255))")
    spark.sql("insert overwrite table partit_bev_br partition(branch) " +
      "select * from bev_branch_full")

    drop_view("bev_br4_7")
    create_view("create view bev_br4_7 as (" +
      "select distinct b4.bev_type " +
      "from (select distinct * from partit_bev_br where branch='Branch4') as b4 inner join " +
      "(select distinct * from partit_bev_br where branch='Branch7') as b7 " +
      "where b4.bev_type = b7.bev_type)")
  }

  def problem_scenario_4(): Unit = {
    prob_scen_4_setup()
    println("Problem Scenario 4:")
    println("Recreating Scenario 3 using partitions and views.")

    println("The beverages available on branch 1 are:")
    spark.sql("select distinct * from partit_bev_br where branch='Branch1'").show(100)

    println("The beverages available on branch 8 are:")
    spark.sql("select distinct * from partit_bev_br where branch='Branch8'").show(100)

    println("The beverages available on branch 10 are:")
    spark.sql("select distinct * from partit_bev_br where branch='Branch10'").show(100)

    println("The beverages that are on both branch 4 and 7 are:")
    spark.sql("select * from bev_br4_7").show(100)

  }

  def problem_scenario_5(): Unit = {
    spark.sql("alter table bev_branch_full set tblproperties ('comment' = 'test comment')")
    spark.sql("alter table bev_branch_full set tblproperties ('notes' = 'test note')")
    spark.sql("describe formatted bev_branch_full").show(50, truncate = 100)
  }

  def problem_scenario_6(): Unit = {
    drop_view("bev_br4_7")
    drop_table("bev_br4_7_v2")
    create_view("create view bev_br4_7 as (" +
      "select distinct b4.bev_type " +
      "from (select distinct * from partit_bev_br where branch='Branch4') as b4 inner join " +
      "(select distinct * from partit_bev_br where branch='Branch7') as b7 " +
      "where b4.bev_type = b7.bev_type)")

    println("Problem Scenario 6:")
    println("Removing a row from the bev_br4_7 view")
    spark.sql("select * from bev_br4_7 order by bev_type asc limit 5").show()
    spark.sql("create table bev_br4_7_v2 as select * from bev_br4_7 where bev_type != \"Cold_Coffee\"")
    spark.sql("select * from bev_br4_7_v2 order by bev_type asc limit 5").show()
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
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
  }

  def spark_test(): Unit = {
    drop_table("newone")
    create_table("newone(id Int,name String)")
    load_local_table_data("newone","test.txt")
    show_all_data("newone")
  }

  def app_init(): Unit = {
    remove_all_data() //Used to reset the base tables
    create_base_tables()
    load_base_data()
    group_similar_base_tables() //Two views called bev_branch_full & bev_conscount_total
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

  def group_similar_base_tables(): Unit = {

    drop_table("bev_branch_full")
    spark.sql("create table if not exists bev_branch_full as " +
      "select * from bev_branch_a union all " +
      "select * from bev_branch_b union all " +
      "select * from bev_branch_c")

    //spark.sql("select count(bev_type) as A_total_rows from bev_branch_a").show()
    //spark.sql("select count(bev_type) as B_total_rows from bev_branch_b").show()
    //spark.sql("select count(bev_type) as C_total_rows from bev_branch_c").show()
    //spark.sql("select count(bev_type) as Total_Rows from bev_branch_full").show()


    drop_table("bev_conscount_total")
    spark.sql("create table if not exists bev_conscount_total as " +
      "select * from bev_conscount_a union all " +
      "select * from bev_conscount_b union all " +
      "select * from bev_conscount_c");

    //spark.sql("select count(consumer_count) as A_total_rows from bev_conscount_a").show()
    //spark.sql("select count(consumer_count) as B_total_rows from bev_conscount_b").show()
    //spark.sql("select count(consumer_count) as C_total_rows from bev_conscount_c").show()
    //spark.sql("select count(consumer_count) as Total_Rows from bev_conscount_total").show()
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
      spark.sql("SELECT * FROM " + name).show(100)
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