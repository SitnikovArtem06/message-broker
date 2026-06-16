package core

type DeadLetter struct {
	Message     Message
	SourceQueue string
	Reason      string
	Attempts    int
}
