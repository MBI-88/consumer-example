package internal

import (
	"log"
	"os"
	"time"

	"github.com/MBI-88/dominus-proto-definition/dominus"
	sdk "github.com/MBI-88/dominus-sdk/dominus"
)

type Consumer interface {
	GetMessage(stop <-chan os.Signal)
}

type consumer struct {
	sqs      sdk.Sqs
	workerId string
	groupId  string
}

func NewConsumer(sqs sdk.Sqs, workId, groupId string) Consumer {
	return &consumer{
		sqs:      sqs,
		workerId: workId,
		groupId:  groupId,
	}
}

func (c *consumer) GetMessage(stop <-chan os.Signal) {
	clock := time.NewTicker(10*time.Second)

	for {
		select {
		case <-clock.C:
			c.doRequest()

		case <-stop:
			return
		}
	}

}

func (c *consumer) doRequest() {
	resp, err := c.sqs.UseConsumer(&dominus.ConsumerRequest{
		WorkerId: c.workerId,
		GroupId:  c.groupId,
	})

	if err != nil {
		log.Println(err)
	}

	var messageID = resp.GetMessageId()

	log.Printf("Response {message_id: %s, created_at: %s, message: %s}",
		messageID,
		resp.GetDate(),
		resp.GetMessage(),
	)

	time.Sleep(15 * time.Second)
	_, err = c.sqs.UseAck(&dominus.ConsumerRequest{
		MessageId: messageID,
		WorkerId:  c.workerId,
		GroupId:   c.groupId,
	})

	if err != nil {
		log.Println(err)
	}
}
