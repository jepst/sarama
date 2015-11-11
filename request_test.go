package sarama

import (
	"bytes"
	"reflect"
	"testing"
)

type testRequestBody struct {
}

func (s *testRequestBody) key() int16 {
	return 0x666
}

func (s *testRequestBody) version() int16 {
	return 0xD2
}

func (s *testRequestBody) Encode(pe packetEncoder) error {
	return pe.putString("abc")
}

// not specific to request tests, just helper functions for testing structures that
// implement the encoder or decoder interfaces that needed somewhere to live

func testEncodable(t *testing.T, name string, in Encoder, expect []byte) {
	packet, err := Encode(in)
	if err != nil {
		t.Error(err)
	} else if !bytes.Equal(packet, expect) {
		t.Error("Encoding", name, "failed\ngot ", packet, "\nwant", expect)
	}
}

func testDecodable(t *testing.T, name string, out Decoder, in []byte) {
	err := Decode(in, out)
	if err != nil {
		t.Error("Decoding", name, "failed:", err)
	}
}

func testRequest(t *testing.T, name string, rb RequestBody, expected []byte) {
	// Encoder request
	req := &Request{CorrelationID: 123, ClientID: "foo", Body: rb}
	packet, err := Encode(req)
	headerSize := 14 + len("foo")
	if err != nil {
		t.Error(err)
	} else if !bytes.Equal(packet[headerSize:], expected) {
		t.Error("Encoding", name, "failed\ngot ", packet, "\nwant", expected)
	}
	// Decoder request
	decoded, err := decodeRequest(bytes.NewReader(packet))
	if err != nil {
		t.Error("Failed to decode request", err)
	} else if decoded.CorrelationID != 123 || decoded.ClientID != "foo" {
		t.Errorf("Decoded header is not valid: %v", decoded)
	} else if !reflect.DeepEqual(rb, decoded.Body) {
		t.Errorf("Decoded request does not match the encoded one\nencoded: %v\ndecoded: %v", rb, decoded)
	}
}

func testResponse(t *testing.T, name string, res Encoder, expected []byte) {
	encoded, err := Encode(res)
	if err != nil {
		t.Error(err)
	} else if expected != nil && !bytes.Equal(encoded, expected) {
		t.Error("Encoding", name, "failed\ngot ", encoded, "\nwant", expected)
	}

	decoded := reflect.New(reflect.TypeOf(res).Elem()).Interface().(Decoder)
	if err := Decode(encoded, decoded); err != nil {
		t.Error("Decoding", name, "failed:", err)
	}

	if !reflect.DeepEqual(decoded, res) {
		t.Errorf("Decoded response does not match the encoded one\nencoded: %#v\ndecoded: %#v", res, decoded)
	}
}
