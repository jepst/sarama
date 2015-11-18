package sarama

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all replicas to commit before responding.
	WaitForAll RequiredAcks = -1
)

type ProduceRequest struct {
	RequiredAcks RequiredAcks
	Timeout      int32
	MsgSets      map[string]map[int32]*MessageSet
}

func (p *ProduceRequest) Encode(pe packetEncoder) error {
	pe.putInt16(int16(p.RequiredAcks))
	pe.putInt32(p.Timeout)
	err := pe.putArrayLength(len(p.MsgSets))
	if err != nil {
		return err
	}
	for topic, partitions := range p.MsgSets {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for id, msgSet := range partitions {
			pe.putInt32(id)
			pe.push(&lengthField{})
			err = msgSet.Encode(pe)
			if err != nil {
				return err
			}
			err = pe.pop()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *ProduceRequest) Decode(pd packetDecoder) error {
	requiredAcks, err := pd.getInt16()
	if err != nil {
		return err
	}
	p.RequiredAcks = RequiredAcks(requiredAcks)
	if p.Timeout, err = pd.getInt32(); err != nil {
		return err
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	p.MsgSets = make(map[string]map[int32]*MessageSet)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		p.MsgSets[topic] = make(map[int32]*MessageSet)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			messageSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}
			msgSetDecoder, err := pd.getSubset(int(messageSetSize))
			if err != nil {
				return err
			}
			msgSet := &MessageSet{}
			err = msgSet.Decode(msgSetDecoder)
			if err != nil {
				return err
			}
			p.MsgSets[topic][partition] = msgSet
		}
	}
	return nil
}

func (p *ProduceRequest) Key() int16 {
	return 0
}

func (p *ProduceRequest) Version() int16 {
	return 0
}

func (p *ProduceRequest) AddMessage(topic string, partition int32, msg *Message) {
	if p.MsgSets == nil {
		p.MsgSets = make(map[string]map[int32]*MessageSet)
	}

	if p.MsgSets[topic] == nil {
		p.MsgSets[topic] = make(map[int32]*MessageSet)
	}

	set := p.MsgSets[topic][partition]

	if set == nil {
		set = new(MessageSet)
		p.MsgSets[topic][partition] = set
	}

	set.addMessage(msg)
}

func (p *ProduceRequest) AddSet(topic string, partition int32, set *MessageSet) {
	if p.MsgSets == nil {
		p.MsgSets = make(map[string]map[int32]*MessageSet)
	}

	if p.MsgSets[topic] == nil {
		p.MsgSets[topic] = make(map[int32]*MessageSet)
	}

	p.MsgSets[topic][partition] = set
}
