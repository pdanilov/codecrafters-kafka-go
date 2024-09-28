package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	CodeInvalidApiVersion = 35
)

type KafkaRequest struct {
	Length        int32
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	Body          []byte
}

func (req *KafkaRequest) Validate() int16 {
	switch req.ApiVersion {
	case 0, 1, 2, 3, 4:
		return 0
	default:
		return int16(CodeInvalidApiVersion)
	}
}

type ApiKey struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}

var (
	Fetch       = ApiKey{Key: 1, MinVersion: 1, MaxVersion: 16}
	ApiVersions = ApiKey{Key: 18, MinVersion: 1, MaxVersion: 4}
)

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

type KafkaConn struct {
	net.Conn
}

func NewKafkaConn(conn net.Conn) *KafkaConn {
	return &KafkaConn{Conn: conn}
}

func (conn *KafkaConn) Close() error {
	return conn.Conn.Close()
}

func (conn *KafkaConn) Request(ctx context.Context) (*KafkaRequest, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	}

	req := &KafkaRequest{}
	vs := []any{&req.Length, &req.ApiKey, &req.ApiVersion, &req.CorrelationId}
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

	req.Body = make([]byte, int(req.Length)-n)
	if err := binary.Read(conn, binary.BigEndian, &req.Body); err != nil {
		return nil, err
	}

	return req, nil
}

func (conn *KafkaConn) Response(ctx context.Context, resp *KafkaResponse) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}

	for _, v := range []any{resp.Length, resp.CorrelationId, resp.ErrorCode, resp.NumOfApiKeys} {
		err := binary.Write(conn, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	for _, key := range resp.ApiKeys {
		for _, v := range []any{key.Key, key.MinVersion, key.MaxVersion, []byte{0}} {
			err := binary.Write(conn, binary.BigEndian, v)
			if err != nil {
				return err
			}
		}
	}

	for _, v := range []any{resp.ThrottleTime, []byte{0}} {
		err := binary.Write(conn, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (conn *KafkaConn) Handle(ctx context.Context) error {
	req, err := conn.Request(ctx)
	if err != nil {
		return nil
	}

	resp := NewKafkaResponse(req.CorrelationId, req.Validate(), []ApiKey{Fetch, ApiVersions}, 0)
	if err := conn.Response(ctx, resp); err != nil {
		return err
	}

	return nil
}

func (conn *KafkaConn) HandleLoop(ctx context.Context) error {
	for {
		err := func() error {
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
			defer cancel()
			return conn.Handle(ctx)
		}()

		if err != nil {
			return err
		}
	}
}

type KafkaServer struct {
	listener net.Listener
}

func NewKafkaServer(port string) (*KafkaServer, error) {
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		return nil, err
	}

	srv := &KafkaServer{listener: l}
	return srv, nil
}

func (srv *KafkaServer) Run() error {
	g, ctx := errgroup.WithContext(context.Background())

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			g.Go(func() error {
				c, err := srv.listener.Accept()
				if err != nil {
					return fmt.Errorf("Error accepting connection: %w", err)
				}

				conn := NewKafkaConn(c)
				defer conn.Close()
				if err := conn.HandleLoop(ctx); err != nil {
					return fmt.Errorf("Error while handling request: %w", err)
				}

				return nil
			})
		}
	}

	return g.Wait()
}

func (srv *KafkaServer) Shutdown() error {
	return srv.listener.Close()
}
