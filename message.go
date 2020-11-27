package mx

type Message interface {
	Topic() string

	Value() []byte
}
