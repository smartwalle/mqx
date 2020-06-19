package mx

type Message interface {
	Value() []byte

	Topic() string
}
