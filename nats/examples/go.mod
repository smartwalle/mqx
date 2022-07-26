module github.com/smartwalle/mx/nats/examples

go 1.13

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/nats-io/nats-server/v2 v2.8.4 // indirect
	github.com/smartwalle/mx v0.0.4
	github.com/smartwalle/mx/nats v0.0.0
	google.golang.org/protobuf v1.28.0 // indirect
)

replace github.com/smartwalle/mx/nats => ../
