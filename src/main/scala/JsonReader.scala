import com.datastax.oss.driver.api.core.CqlSession
import play.api.libs.json._
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import scala.io.{Codec, Source}

object JsonReader {
  def main(args: Array[String]): Unit = {
    ///////////////////////////////// Reading the students data from a JSON file /////////////////////////////////
    // The path to the students JSON file.
    val jsonFilePath = "src/main/scala/StudentsRecords.json"

    // Reading the JSON file content as a String.
    val jsonString = scala.util.Using.resource(Source.fromFile(jsonFilePath)(Codec.UTF8)) { source =>
      source.mkString
    }

    // Parsing the JSON string.
    val json = Json.parse(jsonString)

    // Number of students retrieved from the json file.
    val numOfStudents = json.as[JsArray].value.size

    // Arrays to store the students records.
    val ids = new Array[Int](numOfStudents)
    val names = new Array[String](numOfStudents)
    val ages = new Array[Int](numOfStudents)
    val cities = new Array[String](numOfStudents)
    val yearsOfStudy = new Array[Int](numOfStudents)
    val scores = new Array[Seq[(String, Int)]](numOfStudents)
    val grades = new Array[Map[String, String]](numOfStudents)
    val GPAs = new Array[Float](numOfStudents)
    val attendances = new Array[Int](numOfStudents)
    val incomes = new Array[Int](numOfStudents)
    val studentsExpenses = new Array[Map[String, Int]](numOfStudents)
    val siblingsNums = new Array[Int](numOfStudents)
    val childrenNums = new Array[Int](numOfStudents)
    val finishedCredits = new Array[Int](numOfStudents)
    val languages = new Array[Set[String]](numOfStudents)
    val healthConditions = new Array[Seq[String]](numOfStudents)

    // Iterate over the arrays of the students.
    for (i <- 0 until numOfStudents) {
      val studentPath = JsPath \ i

      // Extract fields using JsPath for the student.
      val idPath = studentPath \ "id"
      val namePath = studentPath \ "name"
      val agePath = studentPath \ "age"
      val cityPath = studentPath \ "city"
      val yearPath = studentPath \ "Year Of Study"
      val scoresPath = studentPath \ "scores"
      val gradesPath = studentPath \ "grades"
      val gpaPath = studentPath \ "GPA"
      val attendancePath = studentPath \ "attendance"
      val incomePath = studentPath \ "family income"
      val expensesPath = studentPath \ "student expenses"
      val siblingsNumPath = studentPath \ "siblings"
      val childrenNumPath = studentPath \ "children"
      val finishedCredsPath = studentPath \ "finished credits"
      val languagesPath = studentPath \ "languages"
      val healthConditionsPath = studentPath \ "health_conditions"

      // Read values using the JsResult and JsPath.
      ids(i) = json.validate(idPath.read[Int]).getOrElse(-1) // Provide a default value if extraction fails.
      names(i) = json.validate(namePath.read[String]).getOrElse("")
      cities(i) = json.validate(cityPath.read[String]).getOrElse("")
      yearsOfStudy(i) = json.validate(yearPath.read[Int]).getOrElse(-1)
      val scoresJson = json.validate(scoresPath.read[Seq[JsObject]]).getOrElse(Seq.empty)
      scores(i) = scoresJson.map { scoreObj =>
        val subject = (scoreObj \ "subject").validate[String].getOrElse("")
        val score = (scoreObj \ "score").validate[Int].getOrElse(-1)
        (subject, score)
      }
      grades(i) = json.validate(gradesPath.read[Map[String, String]]).getOrElse(Map.empty)
      ages(i) = json.validate(agePath.read[Int]).getOrElse(-1)
      GPAs(i) = json.validate(gpaPath.read[Float]).getOrElse(-1.0f)
      attendances(i) = json.validate(attendancePath.read[Int]).getOrElse(-1)
      incomes(i) = json.validate(incomePath.read[Int]).getOrElse(-1)
      studentsExpenses(i) = json.validate(expensesPath.read[Map[String, Int]]).getOrElse(Map.empty)
      siblingsNums(i) = json.validate(siblingsNumPath.read[Int]).getOrElse(-1)
      childrenNums(i) = json.validate(childrenNumPath.read[Int]).getOrElse(-1)
      finishedCredits(i) = json.validate(finishedCredsPath.read[Int]).getOrElse(-1)
      languages(i) = json.validate(languagesPath.read[Set[String]]).getOrElse(Set.empty)
      healthConditions(i) = json.validate(healthConditionsPath.read[Seq[String]]).getOrElse(Seq.empty)
    }

    // Print the arrays to ensure that I have interpreted them correctly.
    for (i <- 0 until numOfStudents) {
      println(s"ID: ${ids(i)}")
      println(s"Name: ${names(i)}")
      println(s"Age: ${ages(i)}")
      println(s"City: ${cities(i)}")
      println(s"Year Of Study: ${yearsOfStudy(i)}")
      println(s"Scores: ${scores(i)}")
      println(s"Grades: ${grades(i).mkString("[", ", ", "]")}")
      println(s"GPA: ${GPAs(i)}")
      println(s"Attendance: ${attendances(i)}")
      println(s"Family Income: ${incomes(i)}")
      println(s"Student Expenses: ${studentsExpenses(i)}")
      println(s"Siblings: ${siblingsNums(i)}")
      println(s"Children: ${childrenNums(i)}")
      println(s"Finished Credits: ${finishedCredits(i)}")
      println(s"Languages: ${languages(i).mkString("[", ", ", "]")}")
      println(s"Health Conditions: ${healthConditions(i).mkString("[", ", ", "]")}")
      println()
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////


    //////////////////////////////// Creating a connection with Cassandra DB ////////////////////////////////////
    val session = CqlSession.builder()
      .addContactPoint(new InetSocketAddress("localhost", 9042))
      .withLocalDatacenter("datacenter1")
      .build()

    // Use the studentsLog keyspace.
    session.execute("USE studentsLog")
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /////////////////////////// Inserting the obtained data from JSON to Cassandra DB ///////////////////////////
    val insertStatement =
      """
        |INSERT INTO ce_students (
        |  id,
        |  year_of_study,
        |  age,
        |  attendance,
        |  children,
        |  city,
        |  finished_credits,
        |  gpa,
        |  grades,
        |  health_conditions,
        |  income,
        |  languages,
        |  name,
        |  scores,
        |  siblings,
        |  student_expenses
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """.stripMargin

    for(i <- 0 until numOfStudents) {
      session.execute(
        insertStatement,
        ids(i),
        yearsOfStudy(i),
        ages(i),
        attendances(i),
        childrenNums(i),
        cities(i),
        finishedCredits(i),
        GPAs(i),
        grades(i).asJava, // Use scala.jdk.CollectionConverters.
        healthConditions(i).asJava,
        incomes(i),
        languages(i).asJava,
        names(i),
        scores(i).toMap.asJava, // Convert List of tuples to Map and then to Java collection.
        siblingsNums(i),
        studentsExpenses(i).asJava
      )
    }
    // Close the session.
    session.close()
  }
}