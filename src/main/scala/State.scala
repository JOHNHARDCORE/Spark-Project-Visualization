object State {
	private var continue = true
	private var logged_in = false
	private var save = true
	private var user: User = new User()

	def SetContinue(new_state: Boolean) { this.continue = new_state }
	def GetContinue(): Boolean = { this.continue }

	def SetUser(user: User) { this.user = user }
	def GetUser(): User = { this.user }

	def SetLoggedIn(new_state: Boolean) { this.logged_in = new_state }
	def GetLoggedIn(): Boolean = { this.logged_in }

	def SetSave(new_state: Boolean) { this.save = new_state }
	def GetSave(): Boolean = { this.save }
}