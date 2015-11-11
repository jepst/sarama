package sarama

type OffsetFetchRequest struct {
	ConsumerGroup string
	IVersion       int16
	partitions    map[string][]int32
}

func (r *OffsetFetchRequest) Encode(pe packetEncoder) (err error) {
	if r.IVersion < 0 || r.IVersion > 1 {
		return PacketEncodingError{"invalid or unsupported OffsetFetchRequest version field"}
	}

	if err = pe.putString(r.ConsumerGroup); err != nil {
		return err
	}
	if err = pe.putArrayLength(len(r.partitions)); err != nil {
		return err
	}
	for topic, partitions := range r.partitions {
		if err = pe.putString(topic); err != nil {
			return err
		}
		if err = pe.putInt32Array(partitions); err != nil {
			return err
		}
	}
	return nil
}

func (r *OffsetFetchRequest) Decode(pd packetDecoder) (err error) {
	if r.ConsumerGroup, err = pd.getString(); err != nil {
		return err
	}
	partitionCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if partitionCount == 0 {
		return nil
	}
	r.partitions = make(map[string][]int32)
	for i := 0; i < partitionCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitions, err := pd.getInt32Array()
		if err != nil {
			return err
		}
		r.partitions[topic] = partitions
	}
	return nil
}

func (r *OffsetFetchRequest) Key() int16 {
	return 9
}

func (r *OffsetFetchRequest) Version() int16 {
	return r.IVersion
}

func (r *OffsetFetchRequest) AddPartition(topic string, partitionID int32) {
	if r.partitions == nil {
		r.partitions = make(map[string][]int32)
	}

	r.partitions[topic] = append(r.partitions[topic], partitionID)
}
