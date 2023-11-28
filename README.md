# SparkWithCassandra - RDDs
### Task Overview
In this task, a JSON file with 1000 student records is processed using Scala, Apache Spark, and Cassandra. The "JsonReader.scala" script reads and stores student data in Cassandra, and the subsequent "SparkManipulations.scala" script uses Spark for analytics. This includes categorizing students based on GPAs, identifying those who passed, failed, received 33% and 50% scholarships, and achieved recognition in the honor list. Additionally, the project calculates the number of students per academic year. Within this task, I have three files, JsonReader.scala, SparkManipulations.scala, and StudentsRecords.json where the records of 1000 students reside.

## JsonReader
### Overview
The "JsonReader" Scala script is designed to read student data from a JSON file, parse the information using the Play JSON library, and insert the data into a Cassandra database.

### Functionalities
#### 1. JSON Data Reading and Parsing:
Reads a JSON file ("StudentsRecords.json") into a string using the Play JSON library.
Parses the JSON string into a Play JSON object, determining the number of students in the dataset.
Initializes arrays to store student attributes and extracts relevant information using Play JSON's JsPath.
#### 2. Cassandra Database Interaction:
Establishes a connection to a local Cassandra instance using the DataStax Java Driver.
Selects the keyspace "studentsLog" for Cassandra operations.
Defines and executes a prepared INSERT statement for the "ce_students" table, inserting student data into the Cassandra database.
Closes the Cassandra session after data insertion.
### Dependencies
1. Play JSON: Used for parsing JSON data.
2. DataStax Java Driver: Facilitates interaction with the Cassandra database.


## SparkManipulations
### Overview:
The "SparkManipulations" Scala script demonstrates the integration of Apache Spark with Cassandra for comprehensive data manipulations, analysis, and storage.

### Key Features
#### 1. Cassandra Connection
Establishes a connection to a Cassandra database running on localhost:9042.
Utilizes the "studentsLog" keyspace for data operations.
#### 2. Cassandra Data Retrieval
Executes a SELECT query to retrieve all fields from the "ce_students" table.
Processes the retrieved data using RDDs.
#### 3. Spark RDDs
Creates a Spark session for data processing. Converts the retrieved data into an RDD (Resilient Distributed Dataset) named "studentsDataRDD."
Displays the number of students in the RDD using the `count()` action, and perform other manipulations on this RDD.
#### 4. Data Manipulations
- Identifies honor list students (GPA > 3.5) and saves them to the "honor_list" Cassandra table.
- Selects and saves passed students (GPA >= 1.5) to the "passed_students" Cassandra table.
- Identifies and saves failed students (GPA < 1.5) to the "failed_students" Cassandra table.
- Segregates students into scholarship categories and saves to respective Cassandra tables.
- Computes the number of students per academic year using RDD transformations. Saves the result to the "students_per_academic_year" Cassandra table.
#### 5. Resources Cleanup
Closes the Spark session and Cassandra session after completing data operations.
