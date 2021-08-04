import org.apache.spark.sql.SparkSession

object App {

  var spark:SparkSession = null

  def main(args: Array[String]): Unit = {
    spark = spark_session_init()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR") //Remove some of the logging from the spark Session logger
    //spark_test()
    //remove_all_data()
    //load_data()
    //show_original_data()
    //group_similar_tables()
    problem_scenario_1()
    problem_scenario_2()
    problem_scenario_3()
    problem_scenario_4()
    problem_scenario_5()
    problem_scenario_6()

  }

  def problem_scenario_1(): Unit = {
    println("Problem Scenario 1:")
    println("The total number of consumers for Branch 1 is: ")
    //spark.sql("drop view bev_branch1")
    //spark.sql("create view bev_branch1 as select distinct bev_type, branch_num from bev_branch_a where branch_num = \"Branch1\"")
    //spark.sql("select * from bev_branch1").show()

    spark.sql("drop view bev_branch1_cons")
    spark.sql("create view bev_branch1_cons as " +
      "select bev.bev_type, bev.branch_num, con.consumer_count " +
      "from bev_branch1 bev inner join bev_conscount_total con " +
      "on bev.bev_type = con.bev_type")

    //spark.sql("select * from bev_branch1_cons").show()

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
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/test.txt' INTO TABLE src")
    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    //spark.sql("create table newone(id Int,name String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/test.txt' INTO TABLE newone")
    spark.sql("SELECT * FROM newone").show()
  }

  def load_data(): Unit = {
    spark.sql("create table bev_branch_a(bev_type VARCHAR(255), branch_num VARCHAR(255)) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bev_branch_a")

    spark.sql("create table bev_branch_b(bev_type VARCHAR(255), branch_num VARCHAR(255)) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bev_branch_b")

    spark.sql("create table bev_branch_c(bev_type VARCHAR(255), branch_num VARCHAR(255)) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bev_branch_c")

    //Conscount is the consumer count

    spark.sql("create table bev_conscount_a(bev_type VARCHAR(255), consumer_count INT) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bev_conscount_a")

    spark.sql("create table bev_conscount_b(bev_type VARCHAR(255), consumer_count INT) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bev_conscount_b")

    spark.sql("create table bev_conscount_c(bev_type VARCHAR(255), consumer_count INT) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bev_conscount_c")
  }

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

  def remove_all_data(): Unit = {
    spark.sql("drop table bev_branch_a")
    spark.sql("drop table bev_branch_b")
    spark.sql("drop table bev_branch_c")
    spark.sql("drop table bev_conscount_a")
    spark.sql("drop table bev_conscount_b")
    spark.sql("drop table bev_conscount_c")
  }

  def show_original_data(): Unit = {
    spark.sql("SELECT * FROM bev_branch_a").show()
    spark.sql("SELECT * FROM bev_branch_b").show()
    spark.sql("SELECT * FROM bev_branch_c").show()
    spark.sql("SELECT * FROM bev_conscount_a").show()
    spark.sql("SELECT * FROM bev_conscount_b").show()
    spark.sql("SELECT * FROM bev_conscount_c").show()
  }

}