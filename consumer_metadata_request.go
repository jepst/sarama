package sarama

type ConsumerMetadataRequest struct {
	ConsumerGroup string
}

func (r *ConsumerMetadataRequest) Encode(pe packetEncoder) error {
	return pe.putString(r.ConsumerGroup)
}

func (r *ConsumerMetadataRequest) Decode(pd packetDecoder) (err error) {
	r.ConsumerGroup, err = pd.getString()
	return err
}

func (r *ConsumerMetadataRequest) Key() int16 {
	return 10
}

func (r *ConsumerMetadataRequest) Version() int16 {
	return 0
}
