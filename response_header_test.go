package sarama

import "testing"

var (
	responseHeaderBytes = []byte{
		0x00, 0x00, 0x0f, 0x00,
		0x0a, 0xbb, 0xcc, 0xff}
)

func TestResponseHeader(t *testing.T) {
	header := ResponseHeader{}

	testDecodable(t, "response header", &header, responseHeaderBytes)
	if header.Length != 0xf00 {
		t.Error("Decoding header length failed, got", header.Length)
	}
	if header.CorrelationID != 0x0abbccff {
		t.Error("Decoding header correlation id failed, got", header.CorrelationID)
	}
}
