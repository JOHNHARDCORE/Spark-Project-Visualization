class User(private var username: String = "Guest", private var password: String = "password", private var is_admin: Boolean = false) {
	private var id: Int = -1

	def this(id: Int, username: String, password: String) {
		this(username, password, false)
		this.id = id
	}

	def this(id: Int, username: String, password: String, admin: Boolean) {
		this(username, password, admin)
		this.id = id
	}

	def GetID(): Int = { this.id }
	def SetID(id: Int) { this.id = id }

	def GetAdmin(): Boolean = { this.is_admin }
	def SetAdmin(new_state: Boolean) { this.is_admin = new_state }

	def GetUsername(): String = { this.username }
	def SetUsername(username: String) { this.username = username }

	def GetPassword(): String = { this.password }
	def SetPassword(password: String) { this.password = password }

	def PrintInformation() {
		println("---------------------------------")
		println(s"Username: ${this.GetUsername()}")
		if (this.GetAdmin()) {
			println(s"Admin: true")
		}
		println(s"Password: ${this.GetPassword()}")
		println("---------------------------------\n")
	}

}