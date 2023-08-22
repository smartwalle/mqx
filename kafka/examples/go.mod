module github.com/smartwalle/mx/kafka/examples

go 1.13

require (
	github.com/IBM/sarama v1.41.0
	github.com/smartwalle/mx v0.0.6 // indirect
	github.com/smartwalle/mx/kafka v0.0.0
)

replace github.com/smartwalle/mx/kafka => ../
