package sarama

// ReceiveTime is a special value for the timestamp field of Offset Commit Requests which
// tells the broker to set the timestamp to the time at which the request was received.
// The timestamp is only used if message version 1 is used, which requires kafka 0.8.2.
const ReceiveTime int64 = -1

type offsetCommitRequestBlock struct {
	Offset    int64
	Timestamp int64
	Metadata  string
}

func (r *offsetCommitRequestBlock) Encode(pe packetEncoder, version int16) error {
	pe.putInt64(r.Offset)
	if version == 1 {
		pe.putInt64(r.Timestamp)
	} else if r.Timestamp != 0 {
		Logger.Println("Non-zero timestamp specified for OffsetCommitRequest not v1, it will be ignored")
	}

	return pe.putString(r.Metadata)
}

func (r *offsetCommitRequestBlock) Decode(pd packetDecoder, version int16) (err error) {
	if r.Offset, err = pd.getInt64(); err != nil {
		return err
	}
	if version == 1 {
		if r.Timestamp, err = pd.getInt64(); err != nil {
			return err
		}
	}
	r.Metadata, err = pd.getString()
	return err
}

type OffsetCommitRequest struct {
	ConsumerGroup           string
	ConsumerGroupGeneration int32  // v1 or later
	ConsumerID              string // v1 or later
	RetentionTime           int64  // v2 or later

	// Version can be:
	// - 0 (kafka 0.8.1 and later)
	// - 1 (kafka 0.8.2 and later)
	// - 2 (kafka 0.8.3 and later)
	IVersion int16
	Blocks  map[string]map[int32]*offsetCommitRequestBlock
}

func (r *OffsetCommitRequest) Encode(pe packetEncoder) error {
	if r.IVersion < 0 || r.IVersion > 2 {
		return PacketEncodingError{"invalid or unsupported OffsetCommitRequest version field"}
	}

	if err := pe.putString(r.ConsumerGroup); err != nil {
		return err
	}

	if r.IVersion >= 1 {
		pe.putInt32(r.ConsumerGroupGeneration)
		if err := pe.putString(r.ConsumerID); err != nil {
			return err
		}
	} else {
		if r.ConsumerGroupGeneration != 0 {
			Logger.Println("Non-zero ConsumerGroupGeneration specified for OffsetCommitRequest v0, it will be ignored")
		}
		if r.ConsumerID != "" {
			Logger.Println("Non-empty ConsumerID specified for OffsetCommitRequest v0, it will be ignored")
		}
	}

	if r.IVersion >= 2 {
		pe.putInt64(r.RetentionTime)
	} else if r.RetentionTime != 0 {
		Logger.Println("Non-zero RetentionTime specified for OffsetCommitRequest version <2, it will be ignored")
	}

	if err := pe.putArrayLength(len(r.Blocks)); err != nil {
		return err
	}
	for topic, partitions := range r.Blocks {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			if err := block.Encode(pe, r.IVersion); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetCommitRequest) Decode(pd packetDecoder) (err error) {
	if r.ConsumerGroup, err = pd.getString(); err != nil {
		return err
	}

	if r.IVersion >= 1 {
		if r.ConsumerGroupGeneration, err = pd.getInt32(); err != nil {
			return err
		}
		if r.ConsumerID, err = pd.getString(); err != nil {
			return err
		}
	}

	if r.IVersion >= 2 {
		if r.RetentionTime, err = pd.getInt64(); err != nil {
			return err
		}
	}

	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	r.Blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.Blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			block := &offsetCommitRequestBlock{}
			if err := block.Decode(pd, r.IVersion); err != nil {
				return err
			}
			r.Blocks[topic][partition] = block
		}
	}
	return nil
}

func (r *OffsetCommitRequest) Key() int16 {
	return 8
}

func (r *OffsetCommitRequest) Version() int16 {
	return r.IVersion
}

func (r *OffsetCommitRequest) AddBlock(topic string, partitionID int32, offset int64, timestamp int64, metadata string) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	}

	if r.Blocks[topic] == nil {
		r.Blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
	}

	r.Blocks[topic][partitionID] = &offsetCommitRequestBlock{offset, timestamp, metadata}
}
