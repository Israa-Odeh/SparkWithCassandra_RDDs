case class Student(
                    id: Int,
                    year_of_study: Int,
                    age: Int,
                    attendance: Int,
                    children: Int,
                    city: String,
                    finished_credits: Int,
                    gpa: Float,
                    grades: Map[String, String],
                    health_conditions: List[String],
                    income: Int,
                    languages: Set[String],
                    name: String,
                    scores: Seq[(String, Integer)],
                    siblings: Int,
                    student_expenses: Map[String, Integer]
                  )
