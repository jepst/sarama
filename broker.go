package sarama

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Broker represents a single Kafka broker connection. All operations on this object are entirely concurrency-safe.
type Broker struct {
	id   int32
	IAddr string

	conf          *Config
	correlationID int32
	conn          net.Conn
	connErr       error
	lock          sync.Mutex
	opened        int32

	responses chan ResponsePromise
	done      chan bool
}

type ResponsePromise struct {
	CorrelationID int32
	Packets       chan []byte
	Errors        chan error
}

// NewBroker creates and returns a Broker targetting the given host:port address.
// This does not attempt to actually connect, you have to call Open() for that.
func NewBroker(addr string) *Broker {
	return &Broker{id: -1, IAddr: addr}
}

// Open tries to connect to the Broker if it is not already connected or connecting, but does not block
// waiting for the connection to complete. This means that any subsequent operations on the broker will
// block waiting for the connection to succeed or fail. To get the effect of a fully synchronous Open call,
// follow it by a call to Connected(). The only errors Open will return directly are ConfigurationError or
// AlreadyConnected. If conf is nil, the result of NewConfig() is used.
func (b *Broker) Open(conf *Config) error {
	if conf == nil {
		conf = NewConfig()
	}

	err := conf.Validate()
	if err != nil {
		return err
	}

	if !atomic.CompareAndSwapInt32(&b.opened, 0, 1) {
		return ErrAlreadyConnected
	}

	b.lock.Lock()

	if b.conn != nil {
		b.lock.Unlock()
		Logger.Printf("Failed to connect to broker %s: %s\n", b.IAddr, ErrAlreadyConnected)
		return ErrAlreadyConnected
	}

	go withRecover(func() {
		defer b.lock.Unlock()

		dialer := net.Dialer{
			Timeout:   conf.Net.DialTimeout,
			KeepAlive: conf.Net.KeepAlive,
		}

		if conf.Net.TLS.Enable {
			b.conn, b.connErr = tls.DialWithDialer(&dialer, "tcp", b.IAddr, conf.Net.TLS.Config)
		} else {
			b.conn, b.connErr = dialer.Dial("tcp", b.IAddr)
		}
		if b.connErr != nil {
			b.conn = nil
			atomic.StoreInt32(&b.opened, 0)
			Logger.Printf("Failed to connect to broker %s: %s\n", b.IAddr, b.connErr)
			return
		}

		b.conf = conf
		b.done = make(chan bool)
		b.responses = make(chan ResponsePromise, b.conf.Net.MaxOpenRequests-1)

		if b.id >= 0 {
			Logger.Printf("Connected to broker at %s (registered as #%d)\n", b.IAddr, b.id)
		} else {
			Logger.Printf("Connected to broker at %s (unregistered)\n", b.IAddr)
		}
		go withRecover(b.responseReceiver)
	})

	return nil
}

// Connected returns true if the broker is connected and false otherwise. If the broker is not
// connected but it had tried to connect, the error from that connection attempt is also returned.
func (b *Broker) Connected() (bool, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.conn != nil, b.connErr
}

func (b *Broker) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.conn == nil {
		return ErrNotConnected
	}

	close(b.responses)
	<-b.done

	err := b.conn.Close()

	b.conn = nil
	b.connErr = nil
	b.done = nil
	b.responses = nil

	atomic.StoreInt32(&b.opened, 0)

	if err == nil {
		Logger.Printf("Closed connection to broker %s\n", b.IAddr)
	} else {
		Logger.Printf("Error while closing connection to broker %s: %s\n", b.IAddr, err)
	}

	return err
}

// ID returns the broker ID retrieved from Kafka's metadata, or -1 if that is not known.
func (b *Broker) ID() int32 {
	return b.id
}

func (b *Broker) SetID(id int32) {
	b.id = id
}

// Addr returns the broker address as either retrieved from Kafka's metadata or passed to NewBroker.
func (b *Broker) Addr() string {
	return b.IAddr
}

func (b *Broker) GetMetadata(request *MetadataRequest) (*MetadataResponse, error) {
	response := new(MetadataResponse)

	err := b.sendAndReceive(request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) GetConsumerMetadata(request *ConsumerMetadataRequest) (*ConsumerMetadataResponse, error) {
	response := new(ConsumerMetadataResponse)

	err := b.sendAndReceive(request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) GetAvailableOffsets(request *OffsetRequest) (*OffsetResponse, error) {
	response := new(OffsetResponse)

	err := b.sendAndReceive(request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Produce(request *ProduceRequest) (*ProduceResponse, error) {
	var response *ProduceResponse
	var err error

	if request.RequiredAcks == NoResponse {
		err = b.sendAndReceive(request, nil)
	} else {
		response = new(ProduceResponse)
		err = b.sendAndReceive(request, response)
	}

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Fetch(request *FetchRequest) (*FetchResponse, error) {
	response := new(FetchResponse)

	err := b.sendAndReceive(request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) CommitOffset(request *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	response := new(OffsetCommitResponse)

	err := b.sendAndReceive(request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) FetchOffset(request *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	response := new(OffsetFetchResponse)

	err := b.sendAndReceive(request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) send(rb RequestBody, promiseResponse bool) (*ResponsePromise, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.conn == nil {
		if b.connErr != nil {
			return nil, b.connErr
		}
		return nil, ErrNotConnected
	}

	req := &Request{CorrelationID: b.correlationID, ClientID: b.conf.ClientID, Body: rb}
	buf, err := Encode(req)
	if err != nil {
		return nil, err
	}

	err = b.conn.SetWriteDeadline(time.Now().Add(b.conf.Net.WriteTimeout))
	if err != nil {
		return nil, err
	}

	_, err = b.conn.Write(buf)
	if err != nil {
		return nil, err
	}
	b.correlationID++

	if !promiseResponse {
		return nil, nil
	}

	promise := ResponsePromise{req.CorrelationID, make(chan []byte), make(chan error)}
	b.responses <- promise

	return &promise, nil
}

func (b *Broker) sendAndReceive(req RequestBody, res Decoder) error {
	promise, err := b.send(req, res != nil)

	if err != nil {
		return err
	}

	if promise == nil {
		return nil
	}

	select {
	case buf := <-promise.Packets:
		return Decode(buf, res)
	case err = <-promise.Errors:
		return err
	}
}

func (b *Broker) Decode(pd packetDecoder) (err error) {
	b.id, err = pd.getInt32()
	if err != nil {
		return err
	}

	host, err := pd.getString()
	if err != nil {
		return err
	}

	port, err := pd.getInt32()
	if err != nil {
		return err
	}

	b.IAddr = net.JoinHostPort(host, fmt.Sprint(port))
	if _, _, err := net.SplitHostPort(b.IAddr); err != nil {
		return err
	}

	return nil
}

func (b *Broker) Encode(pe packetEncoder) (err error) {

	host, portstr, err := net.SplitHostPort(b.IAddr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portstr)
	if err != nil {
		return err
	}

	pe.putInt32(b.id)

	err = pe.putString(host)
	if err != nil {
		return err
	}

	pe.putInt32(int32(port))

	return nil
}

func (b *Broker) responseReceiver() {
	var dead error
	header := make([]byte, 8)
	for response := range b.responses {
		if dead != nil {
			response.Errors <- dead
			continue
		}

		err := b.conn.SetReadDeadline(time.Now().Add(b.conf.Net.ReadTimeout))
		if err != nil {
			dead = err
			response.Errors <- err
			continue
		}

		_, err = io.ReadFull(b.conn, header)
		if err != nil {
			dead = err
			response.Errors <- err
			continue
		}

		decodedHeader := ResponseHeader{}
		err = Decode(header, &decodedHeader)
		if err != nil {
			dead = err
			response.Errors <- err
			continue
		}
		if decodedHeader.CorrelationID != response.CorrelationID {
			// TODO if decoded ID < cur ID, discard until we catch up
			// TODO if decoded ID > cur ID, save it so when cur ID catches up we have a response
			dead = PacketDecodingError{fmt.Sprintf("correlation ID didn't match, wanted %d, got %d", response.CorrelationID, decodedHeader.CorrelationID)}
			response.Errors <- dead
			continue
		}

		buf := make([]byte, decodedHeader.Length-4)
		_, err = io.ReadFull(b.conn, buf)
		if err != nil {
			dead = err
			response.Errors <- err
			continue
		}

		response.Packets <- buf
	}
	close(b.done)
}
