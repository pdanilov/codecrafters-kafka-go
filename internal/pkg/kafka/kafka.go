package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrUnsupportedApiKey = errors.New("Unsupported api key")
)

const (
	TagBuffer        int8  = 0
	ThrottleTimeMock int32 = 0
)

func WriteMany(w io.Writer, data ...any) error {
	for _, v := range data {
		err := binary.Write(w, binary.BigEndian, v)
		if err != nil {
			return err
		}
	}

	return nil
}

type KafkaConn struct {
	net.Conn
	buf *bytes.Buffer
}

func NewKafkaConn(conn net.Conn) *KafkaConn {
	return &KafkaConn{Conn: conn, buf: bytes.NewBuffer(nil)}
}

func (conn *KafkaConn) Flush() error {
	defer conn.buf.Reset()
	return WriteMany(conn, int32(conn.buf.Len()), conn.buf.Bytes())
}

func (conn *KafkaConn) Close() error {
	return conn.Conn.Close()
}

func (conn *KafkaConn) ParseRequest(ctx context.Context) (*Request, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	}

	req := &Request{}
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

// ApiVersions Response (Version: CodeCrafters) => error_code num_of_api_keys [api_keys] throttle_time_ms TAG_BUFFER
// error_code => INT16
// num_of_api_keys => INT8
// api_keys => api_key min_version max_version
//
//	api_key => INT16
//	min_version => INT16
//	max_version => INT16
//	_tagged_fields
//
// throttle_time_ms => INT32
// _tagged_fields
func (conn *KafkaConn) HandleApiVersionsRequest(ctx context.Context, req *Request) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}

	errCode := req.Validate()
	apiKeys := []ApiKey{ApiKeyFetch, ApiKeyApiVersions}
	numOfApiKeys := int8(len(apiKeys) + 1)
	if err := WriteMany(conn.buf, req.CorrelationId, errCode, numOfApiKeys); err != nil {
		return err
	}

	for _, key := range apiKeys {
		vrange, err := key.VersionRange()
		if err != nil {
			return err
		}

		if err := WriteMany(conn.buf, key, vrange.Min, vrange.Max, TagBuffer); err != nil {
			return err
		}
	}

	if err := WriteMany(conn.buf, ThrottleTimeMock, TagBuffer); err != nil {
		return err
	}

	return conn.Flush()
}

// Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER
//
//	throttle_time_ms => INT32
//	error_code => INT16
//	session_id => INT32
//	responses => topic_id [partitions] TAG_BUFFER
//	  topic_id => UUID
//	  partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER
//	    partition_index => INT32
//	    error_code => INT16
//	    high_watermark => INT64
//	    last_stable_offset => INT64
//	    log_start_offset => INT64
//	    aborted_transactions => producer_id first_offset TAG_BUFFER
//	      producer_id => INT64
//	      first_offset => INT64
//	    preferred_read_replica => INT32
//	    records => COMPACT_RECORDS
func (conn *KafkaConn) HandleFetchRequest(ctx context.Context, req *Request) error {
	errCode := req.Validate()
	sessionId := int32(0)
	numResponses := int8(0)
	err := WriteMany(
		conn.buf, req.CorrelationId, TagBuffer, ThrottleTimeMock, errCode, sessionId, numResponses, TagBuffer,
	)
	if err != nil {
		return err
	}

	return conn.Flush()
}

func (conn *KafkaConn) Handle(ctx context.Context) error {
	req, err := conn.ParseRequest(ctx)
	if err != nil {
		return nil
	}

	switch req.ApiKey {
	case ApiKeyFetch:
		return conn.HandleFetchRequest(ctx, req)
	case ApiKeyApiVersions:
		return conn.HandleApiVersionsRequest(ctx, req)
	default:
		return fmt.Errorf("%w: %d", ErrUnsupportedApiKey, req.ApiKey)
	}
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
