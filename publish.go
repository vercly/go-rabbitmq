package rabbitmq

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

// DeliveryMode.  Transient means higher throughput but messages will not be
// restored on broker restart.  The delivery mode of publishings is unrelated
// to the durability of the queues they reside on.  Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode.  Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// Return captures a flattened struct of fields returned by the server when a
// Publishing is unable to be delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
type Return struct {
	amqp.Return
}

// PublishOptions are used to control how data is published
type PublishOptions struct {
	Exchange string
	// Mandatory fails to publish if there are no queues
	// bound to the routing key
	Mandatory bool
	// Immediate fails to publish if there are no consumers
	// that can ack bound to the queue on the routing key
	Immediate   bool
	ContentType string
	// Transient or Persistent
	DeliveryMode uint8
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chManager *channelManager

	notifyFlowChan chan bool

	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	logger logger
}

// PublisherOptions are used to describe a publisher's configuration.
// Logging set to true will enable the consumer to print to stdout
type PublisherOptions struct {
	Logging bool
}

// GetPublisher returns a new publisher with an open channel to the cluster.
// If you plan to enforce mandatory or immediate publishing, those failures will be reported
// on the channel of Returns that you should setup a listener on.
// Flow controls are automatically handled as they are sent from the server, and publishing
// will fail with an error when the server is requesting a slowdown
func GetPublisher(url string, optionFuncs ...func(*PublisherOptions)) (Publisher, <-chan Return, error) {
	options := &PublisherOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	chManager, err := newChannelManager(url, options.Logging)
	if err != nil {
		return Publisher{}, nil, err
	}

	publisher := Publisher{
		chManager:                  chManager,
		notifyFlowChan:             make(chan bool),
		disablePublishDueToFlow:    false,
		disablePublishDueToFlowMux: &sync.RWMutex{},
		logger:                     logger{logging: options.Logging},
	}

	returnAMQPChan := make(chan amqp.Return)
	returnChan := make(chan Return)
	returnAMQPChan = publisher.chManager.channel.NotifyReturn(returnAMQPChan)
	go func() {
		for ret := range returnAMQPChan {
			returnChan <- Return{
				ret,
			}
		}
	}()

	publisher.notifyFlowChan = publisher.chManager.channel.NotifyFlow(publisher.notifyFlowChan)

	go publisher.startNotifyFlowHandler()

	return publisher, returnChan, nil
}

// Publish publishes the provided data to the given routing keys over the connection
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	publisher.disablePublishDueToFlowMux.RLock()
	if publisher.disablePublishDueToFlow {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}
	publisher.disablePublishDueToFlowMux.RUnlock()

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	for _, routingKey := range routingKeys {
		err := publisher.chManager.channel.Publish(
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			amqp.Publishing{
				ContentType:  options.ContentType,
				Body:         data,
				DeliveryMode: options.DeliveryMode,
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (publisher *Publisher) startNotifyFlowHandler() {
	for ok := range publisher.notifyFlowChan {
		publisher.disablePublishDueToFlowMux.Lock()
		publisher.logger.Println("pausing publishing due to flow request from server")
		if ok {
			publisher.disablePublishDueToFlow = false
		} else {
			publisher.disablePublishDueToFlow = true
		}
		publisher.disablePublishDueToFlowMux.Unlock()
		publisher.logger.Println("resuming publishing due to flow request from server")
	}
}