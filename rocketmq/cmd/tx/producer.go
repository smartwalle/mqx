package main

import (
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/smartwalle/mx/rocketmq"
	"sync"
)

func main() {
	var s = NewService()
	userId, err := s.CreateUser("smartwalle", "password")
	if err != nil {
		fmt.Println("创建用户发生错误:", err)
	}
	if userId > 0 {
		fmt.Println("创建用户:", userId)
	}

	select {}
}

type Service struct {
	db       *DB
	txList   map[string]*Tx
	txMu     sync.Mutex
	producer *rocketmq.TxProducer
}

func NewService() *Service {
	var s = &Service{}
	s.db = &DB{}
	s.txList = make(map[string]*Tx)

	var config = rocketmq.NewConfig()
	s.producer, _ = rocketmq.NewTxProducer("tx-queue-test", s, config)
	return s
}

func (this *Service) ExecuteLocalTransaction(message *primitive.Message) primitive.LocalTransactionState {
	fmt.Println(string(message.Body), message.TransactionId)
	this.txMu.Lock()
	var tx = this.txList[message.GetProperty("tx-id")]
	this.txMu.Unlock()

	if tx == nil {
		return primitive.RollbackMessageState
	}

	if err := tx.Commit(); err != nil {
		return primitive.RollbackMessageState
	}

	return primitive.CommitMessageState
}

func (this *Service) CheckLocalTransaction(message *primitive.MessageExt) primitive.LocalTransactionState {
	fmt.Println(message.TransactionId)
	return primitive.UnknowState
}

func (this *Service) CreateUser(username, password string) (userId int64, err error) {
	var tx = this.db.BeginTx()
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	if err = tx.Exec("INSERT INTO user(username, password) VALUES (?, ?)", username, password); err != nil {
		return 0, err
	}

	var txId = username

	this.txMu.Lock()
	this.txList[txId] = tx
	this.txMu.Unlock()

	defer func() {
		this.txMu.Lock()
		delete(this.txList, txId)
		this.txMu.Unlock()
	}()

	result, err := this.producer.Enqueue([]byte(fmt.Sprintf("username:%s, password:%s", username, password)), map[string]string{"tx-id": txId})
	if err != nil {
		return 0, err
	}

	if result.State != primitive.CommitMessageState {
		return 0, errors.New("创建用户失败")
	}

	fmt.Println("消息状态", result.State)
	return 1, nil
}

type Tx struct {
}

func (this *Tx) Commit() error {
	return nil
}

func (this *Tx) Rollback() error {
	return nil
}

func (this *Tx) Exec(s ...string) error {
	return nil
}

type DB struct {
}

func (this *DB) BeginTx() *Tx {
	return &Tx{}
}
