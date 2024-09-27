package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"reflect"
	"time"

	"golang.org/x/sync/errgroup"
)

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

func NewKafkaRequest(ctx context.Context, conn KafkaConn) (*KafkaRequest, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	}

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

func (r *KafkaResponse) Write(ctx context.Context, conn KafkaConn) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}

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

func Handle(ctx context.Context, conn KafkaConn) error {
	req, err := NewKafkaRequest(ctx, conn)
	if err != nil {
		return err
	}

	var resp *KafkaResponse
	code := req.Validate()
	resp = NewKafkaResponse(req.CorrelationId, code, []ApiKey{{Key: 18, MinVersion: 1, MaxVersion: 4}}, 0)

	if err := resp.Write(ctx, conn); err != nil {
		return err
	}

	return nil
}

func HandleLoop(ctx context.Context, conn KafkaConn) error {
	for {
		err := func() error {
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
			defer cancel()
			return Handle(ctx, conn)
		}()

		if err != nil {
			return err
		}
	}
}

func HandleMultiConn(l net.Listener) error {
	g, ctx := errgroup.WithContext(context.Background())

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			g.Go(func() error {
				conn, err := l.Accept()
				if err != nil {
					return fmt.Errorf("Error accepting connection: %w", err)
				}
				defer conn.Close()

				if err := HandleLoop(ctx, conn); err != nil {
					return fmt.Errorf("Error while handling request: %w", err)
				}

				return nil
			})
		}
	}

	return g.Wait()
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	if err := HandleMultiConn(l); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
