package sarama

import "fmt"

type ResponseHeader struct {
	Length        int32
	CorrelationID int32
}

func (r *ResponseHeader) Decode(pd packetDecoder) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 || r.Length > MaxResponseSize {
		return PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", r.Length)}
	}

	r.CorrelationID, err = pd.getInt32()
	return err
}
