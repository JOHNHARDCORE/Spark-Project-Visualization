import org.apache.spark.sql.functions.{avg, count, expr, floor, max, min, sum, when}
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import scala.io.StdIn.readLine
import scala.io.Source
import java.io.FileNotFoundException
import java.security.MessageDigest
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}

object App {

	var DF: DataFrame = null;
	def getEncryptedPassword(password: String): String = {
		MessageDigest
			.getInstance("SHA-256")
			.digest(password.getBytes("UTF-8"))
			.map("%02X".format(_))
			.mkString
	}

	def pollForInput(query: String = ">Enter your Input"): String = {
		val input = readLine(s"$query\n")
		input
	}

	def findUser() {
		val name = pollForInput("Name: ")

		Database.GetUser(name) match {
			case Some(res) => {
				res.PrintInformation()
			}
			case None => {
				println(s"Couldn't find user ${name}")
			}
		}
	}

	def registerUser(): Option[User] = {
		// could conslidate these two
		val name = pollForInput("Name: ")
		val pass = getEncryptedPassword(pollForInput("Password: "))

		var new_user = new User(name, pass)
		try {
			Database.GetUser(name) match {
				case Some(res) => { println("User already exists!\n"); None }
				case None => {
					Database.InsertNewUser(new_user)
					State.SetUser(new_user)
					Some(new_user)
				}
			}
		} catch {
			case e: SQLException => { println("Not saving this session."); State.SetSave(false); None }
		}
	}

	def login() {
		val name = pollForInput("Name: ")
		val pass = getEncryptedPassword(pollForInput("Password: "))

		Database.GetUser(name) match {
			case Some(res) => { 
				if (!(res.GetPassword() == pass && res.GetUsername() == name)) { println("Incorrect login\n"); return }

				State.SetLoggedIn(true)
				State.SetUser(res)
			}
			case None => {
				println(s"Incorrect login\n")
			}
		}
	}

	def exit() {
		println("Exiting...")
		val user = State.GetUser()
		if(State.GetSave() && user.GetID() != -1) {
			Database.SaveUser(user)
		}
		State.SetLoggedIn(false)
		State.SetContinue(false)
	}

	def makeLoggedInAdmin(): Unit = {
		val user = State.GetUser()
		if (user.GetAdmin()) {
			println("You are already admin")
			return
		}

		user.SetAdmin(true)
		println(s"Granted ${user.GetUsername()} admin privileges")
	}

	def connectToDB() = {
		val configs = loadFile("resources/login.txt") 

		val driver = "com.mysql.cj.jdbc.Driver"
		val url = s"jdbc:mysql://${configs("ip")}/${configs("db")}"
		val username = s"${configs("username")}"
		val password = s"${configs("password")}"

		Class.forName(driver)
		Database.SetConnection(url, username, password)
	}

	def loginScreen(): Boolean = {
		println("\nWelcome to Scala Slots. Please log in or create a new user if you're new")
		println("[1]: Log In")
		println("[2]: Register New User")
		println("[3]: Find User")
		println("[4]: Exit")
		val input = pollForInput()
		input match {
			case "1" => { login() }
			case "2" => { registerUser() }
			case "3" => { findUser() }
			case "4" => { exit() }
			case _ => println("Invalid choice\n")
		}

		return false
	}

	def isAdmin(): Boolean = { State.GetLoggedIn() && State.GetUser().GetAdmin() }

	def printAllUsers(): Unit = {
		Database
			.GetAllUsers()
			.foreach(_.PrintInformation())
	}
	def adminMenu(): Boolean = {
		if (!isAdmin()) {
			println("You are not an admin.")
			return false
		}
		println("Administration Menu")
		println("[1]: Get All Users Information")
		println("[2]: Return")

		pollForInput() match {
			case "1" => { printAllUsers() }
			case "2" => { println("Returning to menu.\n") }
			case _ => { println("idk")}
		}

		false
	}

	def runQueries(): Unit = {
		val spark = SparkSession
			.builder
			.appName("hive test")
			.config("spark.sql.warehouse.dir", "C:\\spark\\spark-warehouse")
			.config("spark.master", "local")
			.enableHiveSupport()
			.getOrCreate()

		import spark.implicits._
		import spark.sql

		spark.sparkContext.setLogLevel("ERROR")

		val inputPlayers = "input/players5000.csv"
		val inputTeamValues = "input/nflteams-value.csv"
		val inputKickReturn = "input/Career_Stats_Kick_Return.csv"
		val inputPassing = "input/Career_Stats_Passing.csv"
		val inputReceiving = "input/Career_Stats_Receiving.csv"
		val	inputRushing = "input/Career_Stats_Rushing.csv"
		//    val inputAverageCareerLength = "/home/luka/Documents/Revature/Project2/AverageCareerLengthInNFL.csv"
		val format = "csv"

		val teamValuesSchema = StructType(Array(
			StructField("Team",org.apache.spark.sql.types.StringType,true),
			StructField("Value",IntegerType,true)))

//		val positionsSchema = StructType(Array(
//			StructField("Position",org.apache.spark.sql.types.StringType,true),
//			StructField("PositionAbb",org.apache.spark.sql.types.StringType,true)
//			//      StructField("AverageCareerLength",DoubleType,true)
//		))

		val playersDF = spark.read.option("header",true).option("inferSchema",true).format(format).load(inputPlayers).toDF("Age","BirthPlace","Birthday","College","CurrentStatus","CurrentTeam", "Experience",
			"Height (inches)","High School","Hich School Location","Name","Number","PlayerId","Position", "Weight (lbs)", "YearsPlayed")
		val teamsDF = spark.read.option("header",true).schema(teamValuesSchema).format(format).load(inputTeamValues).toDF("Team", "Value")
		val kickDF = spark.read.option("header", true).option("inferSchema", true).format(format).load(inputKickReturn).toDF("Player Id","Name","Year","Team","Yards_Returned")
		val passDF = spark.read.option("header", true).option("inferSchema", true).format(format).load(inputPassing).toDF("Player Id","Name","Year","Team","Passing_Yards")
		val receiveDF = spark.read.option("header", true).option("inferSchema", true).format(format).load(inputReceiving).toDF("Player Id","Name","Year","Team","Receiving_Yards")
		val rushingDF = spark.read.option("header", true).option("inferSchema", true).format(format).load(inputRushing).toDF("Player Id","Name","Year","Team","Rushing_Yards")
		//    val careerLengthDF = spark.read.option("header",true).schema(positionsSchema).format(format).load(inputAverageCareerLength).toDF("Position", "PositionAbb","AverageCareerLength")

		playersDF.createOrReplaceTempView("players")
		teamsDF.createOrReplaceTempView("teamValues")
		kickDF.createOrReplaceTempView("Career_Stats_Kick_Return")
		passDF.createOrReplaceTempView("Career_Stats_Passing")
		receiveDF.createOrReplaceTempView("Career_Stats_Receiving")
		rushingDF.createOrReplaceTempView("Career_Stats_Rushing")


		// Query 1: What are the top 5 birth states among all active players?
		val sqlDF1 = spark.sql("SELECT * FROM (SELECT * FROM (SELECT RIGHT(BirthPlace,2) AS State, COUNT(PlayerID) AS PlayerCount FROM players GROUP BY State) AS tbl1 ORDER BY tbl1.PlayerCount DESC) AS tbl2 WHERE tbl2.State IS NOT NULL LIMIT 5")
		println("Query 1: ")
		sqlDF1.show()

		// Query 2: Which colleges did the players of the most valuable team go to?
		val sqlDF2 = spark.sql("SELECT DISTINCT college AS Colleges FROM players WHERE players.CurrentTeam IN (SELECT Team FROM teamValues WHERE Value IN (SELECT Max(Value) FROM teamValues))")
		println("Query 2: ")
		sqlDF2.show(30)


		// Query 4: How many of the active players lasted in NFL longer than the average NFL Career length in their position?
		//    val sqlDF4 = spark.sql("SELECT Position, COUNT(PlayerID) FROM players GROUP BY Position")
		//    val sqlDF4 = spark.sql("SELECT players.Name, players.Position, players.NumberOfYears, players.YearsPlayed, AvgCareerLength.AverageCareerLength FROM players JOIN AvgCareerLength ON players.Position = AvgCareerLength.PositionAbb")
		//    val sqlDF4 = spark.sql("SELECT players.Name, players.Position, players.Experience, players.YearsPlayed, players.NumberOfYears FROM players")

		val sqlDF3 = spark.sql("SELECT Text AS Decade, ROUND(AVG(tbl5.Age), 1) AS AverageAge FROM (SELECT tbl4.PlayerId AS PlayerId, tbl4.ActiveYear, tbl4.Age AS Age, CASE WHEN tbl4.ActiveYear >= 2010 THEN '2010s' WHEN tbl4.ActiveYear >= 2000 THEN '2000s'" +
			"WHEN tbl4.ActiveYear >= 1990 THEN '1990s'" +
			"WHEN tbl4.ActiveYear >= 1980 THEN '1980s'" +
			"WHEN tbl4.ActiveYear >= 1970 THEN '1970s'" +
			"WHEN tbl4.ActiveYear >= 1960 THEN '1960s'" +
			"WHEN tbl4.ActiveYear >= 1950 THEN '1950s'" +
			"WHEN tbl4.ActiveYear >= 1940 THEN '1940s'" +
			"WHEN tbl4.ActiveYear >= 1930 THEN '1930s'" +
			"WHEN tbl4.ActiveYear >= 1920 THEN '1920s' ELSE 'Old' END AS " +
			"Text FROM (SELECT tbl3.PlayerId AS PlayerId, tbl3.ActiveYear, (tbl3.ActiveYear - tbl3.BirthYear) " +
			"AS Age FROM (SELECT tbl2.PlayerId AS PlayerID, tbl2.BirthYear, tbl2.sy, tbl2.ey, ((tbl2.ey + tbl2.sy)/2) " +
			"AS ActiveYear FROM (SELECT tbl1.PlayerId AS PlayerId, CAST(tbl1.BirthYear as INT) AS BirthYear, CAST(tbl1.StartYear AS INT) " +
			"AS sy, CAST(tbl1.EndYear AS INT) AS ey FROM (SELECT YearsPlayed, PlayerId, RIGHT(Birthday,4) AS BirthYear, LEFT(YearsPlayed, 4) " +
			"AS StartYear, RIGHT(YearsPlayed, 4) AS EndYear FROM players WHERE YearsPlayed IS NOT NULL) AS tbl1) AS tbl2) AS tbl3) AS tbl4) " +
			"AS tbl5 GROUP BY tbl5.Text ORDER BY tbl5.Text")
		println("Query 3: ")
		sqlDF3.show()
		//   playersDF.printSchema()
		//   val sqlDF2 = spark.sql("SELECT * FROM teamValues")
		//   sqlDF2.show()

		spark.sql("SELECT Year, AVG(Yards_Returned) as Avg_Yards FROM Career_Stats_Kick_Return GROUP BY Year ORDER BY Year ASC").show()

		spark.sql("SELECT Year, AVG(Passing_Yards) as Avg_Completion_Percentage FROM Career_Stats_Passing GROUP BY Year ORDER BY Year ASC").show()

		spark.sql("SELECT Year, AVG(Rushing_Yards) as Avg_Yards FROM Career_Stats_Rushing GROUP BY Year ORDER BY Year ASC").show()

		spark.sql("SELECT Year, AVG(Receiving_Yards) as Avg_Yards FROM Career_Stats_Receiving GROUP BY Year HAVING ifnull(Avg_Yards, 0) > 0 ORDER BY Year ASC").show()
	}
	def mainMenu(): Boolean = {
		println("What would you like to do?")
		if (isAdmin()) {
			println("[a]: Admin Menu")
		}
		println("[1]: Display My Information")
		println("[2]: Make me admin")
		println("[q]: Run Spark Queries")
		println("[3]: Exit")
		val input = pollForInput()
		input match {
			case "1" => { State.GetUser().PrintInformation() }
			case "2" => { makeLoggedInAdmin() }
			case "q" => { runQueries() }
			case "3" => { exit() }
			case "a" => { adminMenu() }
			case _ => println("Invalid choice\n")
		}

		false
	}

	def loadFile(file: String): Map[String, String] = {
		try {
			Source.fromFile(file)
				.getLines
				.map(line => (line.split("=").map(word => word.trim)))
				.map({case Array(first, second) => (first, second)})
				.toMap
		} catch {
			case e: FileNotFoundException => {
				println(s"Couldn't find that file: $file\n")
				println(s"********** Place SQL login details in resource/login.txt **********")
				println(s"********** With format                                   **********")
				println(s"********** ip = your_ip:port                             **********")
				println(s"********** username = your_username                      **********")
				println(s"********** password = your_password                      **********")
				println(s"********** db = your_db                                  **********")
				
				throw e
			}
			case e: Throwable => throw e
		}
	}



	def main(args: Array[String]) {
		println("Starting....\n\n")

//		println("connecting to mysql")
		connectToDB()
//		println("connected to mysql")

		System.setSecurityManager(null)
		System.setProperty("hadoop.home.dir", "C:\\hadoop")

		 do {
		 	val input = State.GetLoggedIn() match {
		 		case true => mainMenu()
		 		case false => loginScreen()
		 	}
		 } while(State.GetContinue())

//		TODO: figure out how to hook up regular sql for user login

	}

}