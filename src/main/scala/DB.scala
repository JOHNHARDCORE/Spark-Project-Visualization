import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.ResultSet
import collection.mutable.ListBuffer

object Database {
	private var connected = false
	private var ip = ""
	private var username = ""
	private var password = ""
	private var connection: Connection = null

	// combine these two now that i know what's what
	def ExecuteUpdate(query: String): Int = {
		if (!State.GetSave()) { return 0 }
		if (!this.GetConnectionStatus()) { println("not connected"); return 0 }
		val connection = this.GetConnection()
		val statement = connection.createStatement()

		try {
			statement.executeUpdate(query)
		} catch {
			case e: SQLException => { println(e); connection.rollback(); throw e }
		} 
	}

	def ExecuteQuery(query: String): Option[ResultSet] = {
		if (!State.GetSave()) { return None }
		if (!this.GetConnectionStatus()) { println("not connected"); return None }
		val connection = this.GetConnection()
		val statement = connection.createStatement()

		try {
			val res = statement.executeQuery(query)
			Some(res)
		} catch {
			case e: SQLException => { println(e); connection.rollback(); throw e }
		} 
	}

	def SetConnection(ip: String, username: String, password: String) {
		try {
			this.connection = DriverManager.getConnection(ip, username, password)

			this.SetConnectionStatus(true)
			this.ip = ip
			this.username = username
			this.password = password
		} catch {
			case a: ClassNotFoundException => a.printStackTrace(); println("Connection status = Failure"); throw a;
			case b: SQLException => b.printStackTrace(); println("Connection status = Failure"); throw b;
		}
	}

	def GetConnectionStatus(): Boolean = { if (this.connection == null) false else this.connected }
	def SetConnectionStatus(new_state: Boolean) { this.connected = new_state }

	def SetConnection(connection: Connection) { this.connection = connection }
	def GetConnection(): Connection = { this.connection }

	def SaveUser(user: User) {
		try {
			val query = s"UPDATE users SET username = '${user.GetUsername()}', password = '${user.GetPassword()}', is_admin = ${user.GetAdmin()} WHERE id = ${user.GetID()};"
			this.ExecuteUpdate(query)
		} catch {
			case e: SQLException => println(e); throw e
		}
	}

	private def searchUser(name: String): Option[ResultSet]= {
		try {
			val query = s"SELECT * FROM users WHERE username = '${name}';"
			this.ExecuteQuery(query) match {
				case Some(res) => { if (!res.isBeforeFirst()) { None } else Some(res) }
				case None => None
			}
		} catch {
			case e: SQLException => throw e
		}
	}

	def GetUser(name: String): Option[User] = {
		try {
			searchUser(name) match {
				case Some(res) => {
					if (res.next()) {
						val usr = new User(res.getInt(1), res.getString(2), res.getString(3), res.getBoolean(4))
						Some(usr)
					} else {
						Some(new User())
					}
				}
				case None => None
			}
		} catch {
			case e: SQLException => println(e); throw e
		}
	}

	def GetAllUsers(): List[User] = {
		try {
			val query = s"SELECT * FROM users"
			this.ExecuteQuery(query) match {
				case Some(res) => {
					var users = new ListBuffer[User]()
					if (res.next()) {
						do {
							val usr = new User(res.getInt(1), res.getString(2), res.getString(3), res.getBoolean(4))
							users += usr
						} while(res.next())
					}
					users.toList
				}
			}
		} catch {
			case e: SQLException => println(e); throw e
		}
	}

	def InsertNewUser(user: User) {
		try {
			val query = s"INSERT INTO users(username, password, is_admin) values ('${user.GetUsername()}', '${user.GetPassword()}', ${user.GetAdmin()});"
			this.ExecuteUpdate(query)
		} catch {
			case e: SQLException => println("Couldn't add new user to DB"); throw e
		}
	}
}