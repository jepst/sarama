package sarama

type fetchRequestBlock struct {
	FetchOffset int64
	MaxBytes    int32
}

func (f *fetchRequestBlock) Encode(pe packetEncoder) error {
	pe.putInt64(f.FetchOffset)
	pe.putInt32(f.MaxBytes)
	return nil
}

func (f *fetchRequestBlock) Decode(pd packetDecoder) (err error) {
	if f.FetchOffset, err = pd.getInt64(); err != nil {
		return err
	}
	if f.MaxBytes, err = pd.getInt32(); err != nil {
		return err
	}
	return nil
}

type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	blocks      map[string]map[int32]*fetchRequestBlock
}

func (f *FetchRequest) Encode(pe packetEncoder) (err error) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(f.MaxWaitTime)
	pe.putInt32(f.MinBytes)
	err = pe.putArrayLength(len(f.blocks))
	if err != nil {
		return err
	}
	for topic, blocks := range f.blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(blocks))
		if err != nil {
			return err
		}
		for partition, block := range blocks {
			pe.putInt32(partition)
			err = block.Encode(pe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FetchRequest) Decode(pd packetDecoder) (err error) {
	if _, err = pd.getInt32(); err != nil {
		return err
	}
	if f.MaxWaitTime, err = pd.getInt32(); err != nil {
		return err
	}
	if f.MinBytes, err = pd.getInt32(); err != nil {
		return err
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	f.blocks = make(map[string]map[int32]*fetchRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		f.blocks[topic] = make(map[int32]*fetchRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			fetchBlock := &fetchRequestBlock{}
			if err = fetchBlock.Decode(pd); err != nil {
				return nil
			}
			f.blocks[topic][partition] = fetchBlock
		}
	}
	return nil
}

func (f *FetchRequest) Key() int16 {
	return 1
}

func (f *FetchRequest) Version() int16 {
	return 0
}

func (f *FetchRequest) AddBlock(topic string, partitionID int32, fetchOffset int64, maxBytes int32) {
	if f.blocks == nil {
		f.blocks = make(map[string]map[int32]*fetchRequestBlock)
	}

	if f.blocks[topic] == nil {
		f.blocks[topic] = make(map[int32]*fetchRequestBlock)
	}

	tmp := new(fetchRequestBlock)
	tmp.MaxBytes = maxBytes
	tmp.FetchOffset = fetchOffset

	f.blocks[topic][partitionID] = tmp
}
