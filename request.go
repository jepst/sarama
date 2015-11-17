package sarama

import (
	"encoding/binary"
	"fmt"
	"io"
)

type RequestBody interface {
	Encoder
	Decoder
	Key() int16
	Version() int16
}

type Request struct {
	CorrelationID int32
	ClientID      string
	Body          RequestBody
}

func (r *Request) Encode(pe packetEncoder) (err error) {
	pe.push(&lengthField{})
	pe.putInt16(r.Body.Key())
	pe.putInt16(r.Body.Version())
	pe.putInt32(r.CorrelationID)
	err = pe.putString(r.ClientID)
	if err != nil {
		return err
	}
	err = r.Body.Encode(pe)
	if err != nil {
		return err
	}
	return pe.pop()
}

func (r *Request) Decode(pd packetDecoder) (err error) {
	var key int16
	if key, err = pd.getInt16(); err != nil {
		return err
	}
	var version int16
	if version, err = pd.getInt16(); err != nil {
		return err
	}
	if r.CorrelationID, err = pd.getInt32(); err != nil {
		return err
	}
	r.ClientID, err = pd.getString()

	r.Body = allocateBody(key, version)
	if r.Body == nil {
		return PacketDecodingError{fmt.Sprintf("unknown request key (%d)", key)}
	}
	return r.Body.Decode(pd)
}

func DecodeRequest(r io.Reader) (req *Request, err error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}

	length := int32(binary.BigEndian.Uint32(lengthBytes))
	if length <= 4 || length > MaxRequestSize {
		return nil, PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	encodedReq := make([]byte, length)
	if _, err := io.ReadFull(r, encodedReq); err != nil {
		return nil, err
	}

	req = &Request{}
	if err := Decode(encodedReq, req); err != nil {
		return nil, err
	}
	return req, nil
}

func allocateBody(key, version int16) RequestBody {
	switch key {
	case 0:
		return &ProduceRequest{}
	case 1:
		return &FetchRequest{}
	case 2:
		return &OffsetRequest{}
	case 3:
		return &MetadataRequest{}
	case 8:
		return &OffsetCommitRequest{IVersion: version}
	case 9:
		return &OffsetFetchRequest{}
	case 10:
		return &ConsumerMetadataRequest{}
	}
	return nil
}
