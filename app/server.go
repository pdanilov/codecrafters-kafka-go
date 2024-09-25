package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

var (
	ErrInvalidApiVersion = errors.New("Invalid API Version")
)

type KafkaConn net.Conn

type KafkaRequest struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
}

func (r *KafkaRequest) ValidateApiversion() error {
	switch r.ApiVersion {
	case 0, 1, 2, 3, 4:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrInvalidApiVersion, r.ApiVersion)
	}
}

func NewKafkaRequestFromBytes(b []byte) *KafkaRequest {
	r := &KafkaRequest{
		Length:        int32(binary.BigEndian.Uint32(b[:4])),
		ApiKey:        int16(binary.BigEndian.Uint16(b[4:6])),
		ApiVersion:    int16(binary.BigEndian.Uint16(b[6:8])),
		CorrelationId: int32(binary.BigEndian.Uint32(b[8:12])),
	}

	return r
}

type KafkaResponse struct {
	Length        int32
	CorrelationId int32
	ErrorCode     int16
}

func NewKafkaResponse(len int32, correlationId int32, errCode int16) *KafkaResponse {
	r := &KafkaResponse{
		Length:        len,
		CorrelationId: correlationId,
		ErrorCode:     errCode,
	}

	return r
}

func (r *KafkaResponse) Bytes() []byte {
	b := make([]byte, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(r.Length))
	b = binary.BigEndian.AppendUint32(b, uint32(r.CorrelationId))
	b = binary.BigEndian.AppendUint16(b, uint16(r.ErrorCode))
	return b
}

func handle(conn KafkaConn) error {
	reqb := make([]byte, 1024)
	if _, err := conn.Read(reqb); err != nil {
		return err
	}

	req := NewKafkaRequestFromBytes(reqb)
	var errCode int16
	if err := req.ValidateApiversion(); err != nil {
		if errors.Is(err, ErrInvalidApiVersion) {
			errCode = 35
		} else {
			return err
		}
	}

	resp := NewKafkaResponse(0, req.CorrelationId, errCode)
	if _, err := conn.Write(resp.Bytes()); err != nil {
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

	if err := handle(conn); err != nil {
		fmt.Println("Error while handling request: ", err.Error())
		os.Exit(1)
	}
}
