package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"reflect"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type KafkaConn net.Conn

var (
	CodeInvalidApiVersion = 35
)

type KafkaRequest struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
}

func (r *KafkaRequest) Validate() int16 {
	switch r.ApiVersion {
	case 0, 1, 2, 3, 4:
		return 0
	default:
		return int16(CodeInvalidApiVersion)
	}
}

func NewKafkaRequest(conn KafkaConn) (*KafkaRequest, error) {
	r := &KafkaRequest{}
	vs := []any{&r.Length, &r.ApiKey, &r.ApiVersion, &r.CorrelationId}
	var n int
	for i, v := range vs {
		err := binary.Read(conn, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}

		if i > 0 {
			n += int(reflect.TypeOf(v).Elem().Size())
		}
	}

	b := make([]byte, int(r.Length)-n)
	if _, err := conn.Read(b); err != nil {
		return nil, err
	}

	return r, nil
}

type ApiKey struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}

type KafkaResponse struct {
	Length        int32
	CorrelationId int32
	ErrorCode     int16
	NumOfApiKeys  int8
	ApiKeys       []ApiKey
	ThrottleTime  int32
}

func NewKafkaResponseShort(correlationId int32, errCode int16) *KafkaResponse {
	r := &KafkaResponse{
		Length:        int32(reflect.TypeOf(correlationId).Size() + reflect.TypeOf(errCode).Size()),
		CorrelationId: correlationId,
		ErrorCode:     errCode,
	}

	return r
}

func NewKafkaResponse(correlationId int32, errCode int16, apiKeys []ApiKey, throttleTime int32) *KafkaResponse {
	numOfApiKeys := int8(len(apiKeys))
	length := int32(
		reflect.TypeOf(correlationId).Size() +
			reflect.TypeOf(errCode).Size() +
			reflect.TypeOf(numOfApiKeys).Size(),
	)

	for _, key := range apiKeys {
		length += int32(
			reflect.TypeOf(key.Key).Size() +
				reflect.TypeOf(key.MinVersion).Size() +
				reflect.TypeOf(key.MaxVersion).Size() +
				1, // TAG_BUFFER
		)
	}

	length += int32(
		reflect.TypeOf(throttleTime).Size() +
			1, // TAG_BUFFER
	)

	r := &KafkaResponse{
		Length:        length,
		CorrelationId: correlationId,
		ErrorCode:     errCode,
		NumOfApiKeys:  numOfApiKeys + 1, // I don't know why do we need +1, it's from code examples
		ApiKeys:       apiKeys,
		ThrottleTime:  throttleTime,
	}

	return r
}

func (r *KafkaResponse) Write(conn KafkaConn) error {
	for _, v := range []any{r.Length, r.CorrelationId, r.ErrorCode, r.NumOfApiKeys} {
		err := binary.Write(conn, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	for _, key := range r.ApiKeys {
		for _, v := range []any{key.Key, key.MinVersion, key.MaxVersion, []byte{0}} {
			err := binary.Write(conn, binary.BigEndian, v)
			if err != nil {
				return err
			}
		}
	}

	for _, v := range []any{r.ThrottleTime, []byte{0}} {
		err := binary.Write(conn, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func Handle(conn KafkaConn) error {
	req, err := NewKafkaRequest(conn)
	if err != nil {
		return err
	}

	var resp *KafkaResponse
	code := req.Validate()
	resp = NewKafkaResponse(req.CorrelationId, code, []ApiKey{{Key: 18, MinVersion: 1, MaxVersion: 4}}, 0)

	if err := resp.Write(conn); err != nil {
		return err
	}

	return nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	if err := Handle(conn); err != nil {
		fmt.Println("Error while handling request: ", err.Error())
		os.Exit(1)
	}
}
