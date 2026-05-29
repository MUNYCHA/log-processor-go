package notification

type Notifier interface {
	Send(message string)
}
