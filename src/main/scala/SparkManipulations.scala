import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer

object SparkManipulations extends App {
  ///////////////////////////////// Create a connection with Cassandra DB /////////////////////////////////////
  val session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("localhost", 9042))
    .withLocalDatacenter("datacenter1")
    .build()

  // Use the studentsLog keyspace.
  session.execute("USE studentsLog")

  // Select all fields from the table.
  private val selectStatement =
    """
      |SELECT * FROM ce_students
      |""".stripMargin

  // A list of Students.
  private var studentsList = ListBuffer[Student]()
  // Execute the select query and process the result.
  private val resultSet = session.execute(selectStatement)
  resultSet.forEach { row =>
    // Using a case class "Student".
    val student = Student(
      // Retrieve and process each field.
      id = row.getInt("id"),
      year_of_study = row.getInt("year_of_study"),
      age = row.getInt("age"),
      attendance = row.getInt("attendance"),
      children = row.getInt("children"),
      city = row.getString("city"),
      finished_credits = row.getInt("finished_credits"),
      gpa = row.getFloat("gpa"),
      grades = row.getMap[String, String]("grades", classOf[String], classOf[String]).asScala.toMap,
      health_conditions = row.getSet[String]("health_conditions", classOf[String]).asScala.toList,
      income = row.getInt("income"),
      languages = row.getSet[String]("languages", classOf[String]).asScala.toSet,
      name = row.getString("name"),
      scores = row.getMap[String, Integer]("scores", classOf[String], classOf[Integer]).asScala.toSeq,
      siblings = row.getInt("siblings"),
      student_expenses = row.getMap[String, Integer]("student_expenses", classOf[String], classOf[Integer]).asScala.toMap,
    )

    // Append the retrieved student to the students list.
    studentsList = studentsList :+ student
    println(student)
    println()
  }

  // Create a spark session.
  private val spark = SparkSession.builder()
    .appName("CassandraDataLoading")
    .master("local")
    .getOrCreate()

  private val sc = spark.sparkContext
  // Convert the students list to an RDD.
  private val studentsDataRDD: RDD[Student] = sc.parallelize(studentsList.toSeq)
  println(s"The number of students is: ${studentsDataRDD.count()}")

  private val keyspace = "studentslog"

  // Choose the students to be included in the honor list.
  private val honorListRDD: RDD[Student] = studentsDataRDD.filter(student => student.gpa > 3.5)
  honorListRDD.foreach(println)

  // Define honor list table.
  private var createTable =
    """
      | CREATE TABLE IF NOT EXISTS honor_list (
      |  id INT PRIMARY KEY,
      |  name TEXT,
      |  year_of_study INT,
      |  gpa FLOAT
      |)
    """.stripMargin

  // Execute the Cassandra creation query.
  session.execute(createTable)

  // Save honorListRDD to Cassandra honor_list table using saveToCassandra.
  honorListRDD.saveToCassandra(keyspace, "honor_list", SomeColumns(
    "id", "name", "year_of_study", "gpa"
  ))

  // List of students who have successfully passed.
  private val passedStudentsRDD: RDD[Student] = studentsDataRDD.filter(student => student.gpa >= 1.5)
  passedStudentsRDD.foreach(println)

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS passed_students (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study TEXT,
      | gpa INT
      | )
      |""".stripMargin
  session.execute(createTable)

  passedStudentsRDD.saveToCassandra(keyspace, "passed_students", SomeColumns("id", "name", "year_of_study", "gpa"))

  // List of students who have Failed.
  private val failedStudentsRDD: RDD[Student] = studentsDataRDD.filter(student => student.gpa < 1.5)
  failedStudentsRDD.foreach(println)

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS failed_students (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study INT,
      | gpa INT
      | )
      |""".stripMargin
  session.execute(createTable)

  failedStudentsRDD.saveToCassandra(keyspace, "failed_students", SomeColumns("id", "name", "year_of_study", "gpa"))

  // List of students who have been awarded a 33% scholarship.
  private val scholarship1RecipientsRDD = studentsDataRDD.filter(student => student.gpa >= 3.5 && student.gpa <= 3.64)
  scholarship1RecipientsRDD.foreach(println)

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS scholarship1_recipients (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study INT,
      | gpa INT
      | )
      |""".stripMargin
  session.execute(createTable)

  scholarship1RecipientsRDD.saveToCassandra(keyspace, "scholarship1_recipients", SomeColumns("id", "name", "year_of_study", "gpa"))

  // List of students who have been awarded a 50% scholarship
  private val scholarship2RecipientsRDD = studentsDataRDD.filter(student => student.gpa > 3.64)
  scholarship2RecipientsRDD.foreach(println)

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS scholarship2_recipients (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study INT,
      | gpa INT
      | )
      |""".stripMargin
  session.execute(createTable)

  scholarship2RecipientsRDD.saveToCassandra(keyspace, "scholarship2_recipients", SomeColumns("id", "name", "year_of_study", "gpa"))

  // Group by "year_of_study" and calculate the count of students per each year.
  private val studentsPerYearRDD: RDD[(Int, Int)] = studentsDataRDD
    .map(student => (student.year_of_study, 1))
    .reduceByKey(_ + _)
    .sortByKey()

  studentsPerYearRDD.foreach { case (year, count) =>
    println(s"Year: $year, Students Count: $count")
  }

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS students_per_academic_year (
      | year_of_study INT PRIMARY KEY,
      | students_count INT
      | )
      |""".stripMargin
  session.execute(createTable)

  studentsPerYearRDD.saveToCassandra(keyspace, "students_per_academic_year", SomeColumns(
    "year_of_study", "students_count"
  ))

  spark.close()
  session.close()
}