package net

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/hashicorp/yamux"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Peer struct {
	logger *zap.SugaredLogger
	quit   chan struct{}

	addr string

	connPool map[string]*yamux.Session
	mu       sync.Mutex

	rqMu            sync.Mutex
	requestHandlers map[string]RequestHandlerFunc
	stMu            sync.Mutex
	streamHandlers  map[string]StreamHandlerFunc
}

func NewPeer(logger *zap.Logger, addr string) *Peer {
	return &Peer{
		quit:     make(chan struct{}),
		logger:   logger.Sugar(),
		connPool: make(map[string]*yamux.Session),
		addr:     addr,

		requestHandlers: make(map[string]RequestHandlerFunc),
		streamHandlers:  make(map[string]StreamHandlerFunc),
	}
}

type RequestHandlerFunc func(Context, []byte, *ResponseWriter)
type StreamHandlerFunc func(Context, chan []byte, *ResponseWriter)

type ResponseWriter struct {
	c   net.Conn
	msg *Message
}

func newResponseWriter(c net.Conn) *ResponseWriter {
	return &ResponseWriter{
		c: c,
		msg: &Message{
			Type:     ResponseMsg,
			Metadata: make(map[string][]byte),
		},
	}
}

func (rw *ResponseWriter) WithMetadata(metadata map[string][]byte) *ResponseWriter {
	rw.msg.Metadata = metadata
	return rw
}

func (rw *ResponseWriter) Response(isOk bool, data []byte) error {
	res := &Response{
		IsError: !isOk,
		Payload: data,
	}

	resBytes := res.Marshal()
	rw.msg.Length = uint32(len(resBytes))
	rw.msg.Version = 1
	rw.msg.Data = resBytes

	msgBytes := rw.msg.Marshal()
	_, err := rw.c.Write(msgBytes)

	rw.c.Close()
	return err
}

type RequestWriter struct {
	peer *Peer
	addr string
	msg  *Message
}

func NewRequestWriter(peer *Peer, addr string) *RequestWriter {
	return &RequestWriter{
		peer: peer,
		addr: addr,
		msg: &Message{
			Type:     RequestMsg,
			Metadata: make(map[string][]byte),
		},
	}
}

func (rw *RequestWriter) WithMetadata(metadata map[string][]byte) *RequestWriter {
	rw.msg.Metadata = metadata
	return rw
}

func (rw *RequestWriter) Request(req *Request) (*Response, error) {
	conn, err := rw.peer.Dial(rw.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	data := req.Marshal()
	rw.msg.Length = uint32(len(data))
	rw.msg.Version = 1
	rw.msg.Data = data

	_, err = conn.Write(rw.msg.Marshal())
	if err != nil {
		return nil, err
	}

	resMsg, err := ParseMessage(conn)
	if err != nil {
		return nil, err
	}

	if resMsg.Type != ResponseMsg {
		return nil, errors.New("Unexpected response type")
	}

	return ParseResponse(resMsg.Data)
}

func (rw *RequestWriter) RequestProto(scope string, req proto.Message, res proto.Message) error {
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	request := &Request{
		Scope:   scope,
		Payload: reqBytes,
	}

	response, err := rw.Request(request)

	if err != nil {
		return err
	}

	if response.IsError {
		return errors.New(fmt.Sprintf("Remote node %s returned error: %s", rw.addr, string(response.Payload)))
	}

	return proto.Unmarshal(response.Payload, res)
}

type StreamWriter struct {
	peer *Peer
	addr string
	msg  *Message
}

func NewStreamWriter(peer *Peer, addr string) *StreamWriter {
	return &StreamWriter{
		peer: peer,
		addr: addr,
		msg: &Message{
			Type:     StreamMsg,
			Metadata: make(map[string][]byte),
		},
	}
}

func (sw *StreamWriter) WithMetadata(metadata map[string][]byte) *StreamWriter {
	sw.msg.Metadata = metadata
	return sw
}

func (sw *StreamWriter) OpenStream(scope string) (net.Conn, error) {
	c, err := sw.peer.Dial(sw.addr)
	if err != nil {
		return nil, err
	}

	stream := &Stream{
		Scope: scope,
	}

	streamBytes := stream.Marshal()
	sw.msg.Length = uint32(len(streamBytes))
	sw.msg.Version = 1
	sw.msg.Data = streamBytes

	_, err = c.Write(sw.msg.Marshal())
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (p *Peer) GetAddr() string {
	return p.addr
}

func (p *Peer) Dial(addr string) (net.Conn, error) {
	p.mu.Lock()
	session, ok := p.connPool[addr]
	p.mu.Unlock()
	if !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}

		session, err = yamux.Client(conn, nil)
		if err != nil {
			return nil, err
		}

		p.mu.Lock()
		p.connPool[addr] = session
		p.mu.Unlock()

		p.handleSession(session)
	}

	stream, err := session.Open()
	if err != nil {
		p.logger.Infow(
			"Closing session with node",
			zap.String("node", addr),
		)
		session.Close()          //return to this later
		delete(p.connPool, addr) //drop the session and redial later?
		return nil, err
	}

	return stream, nil
}

func (p *Peer) Listen(ctx context.Context) error {
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", p.addr)
	if err != nil {
		return err
	}
	p.logger.Infof("Listening on %s", p.addr)

	go func() {
		<-ctx.Done()
		close(p.quit)
		p.logger.Infow("Shutting service down...")
		l.Close()
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-p.quit:
				break
			default:
				p.logger.Errorln("Error accepting connection: %s", err.Error())
			}
		}
		go func() {
			session, err := yamux.Server(c, nil)
			if err != nil {
				p.logger.Errorf("Failed to open a session: %s", err.Error())
				return
			}

			p.mu.Lock()
			p.connPool[c.RemoteAddr().String()] = session
			p.mu.Unlock()

			p.handleSession(session)
			session.Close()
		}()
	}
}

func (p *Peer) handleSession(session *yamux.Session) error {
	remote := session.RemoteAddr()
	for {
		stream, err := session.Accept()
		if err != nil {
			if err == io.EOF {
				break
			}

			if session.IsClosed() {
				break
			}

			p.logger.Errorw(
				fmt.Sprintf("Error while accepting stream: %s", err.Error()),
				"node", remote.String(),
			)

			continue
		}

		go func() {
			err = p.handleRequest(context.Background(), stream)
			if err != nil {
				if err == io.EOF {
					return
				}

				p.logger.Errorw(
					fmt.Sprintf("Error while handling request: %s", err.Error()),
					"node", remote.String(),
				)
			}
		}()
	}

	return nil
}

func (p *Peer) handleRequest(ctx context.Context, c net.Conn) error {
	msg, err := ParseMessage(c)
	if err != nil {
		return err
	}

	context := Context{
		node:     c.RemoteAddr().String(),
		metadata: msg.Metadata,
	}

	rw := newResponseWriter(c)

	switch msg.Type {
	case RequestMsg:
		req, err := ParseRequest(msg.Data)
		if err != nil {
			return err
		}

		context.scope = req.Scope
		p.routeRequest(context, rw, req.Payload)
	case StreamMsg:
		st, err := ParseStream(msg.Data)
		if err != nil {
			return err
		}

		context.scope = st.Scope
		stream := p.handleStream(ctx, c)
		p.routeStream(context, rw, stream)
	default:
		return errors.New("Unsupported request message type")
	}

	return nil
}

func (p *Peer) handleStream(ctx context.Context, c net.Conn) chan []byte {
	stream := make(chan []byte)
	go func() {
		for {
			data, err := readData(c)
			if err != nil {
				close(stream)
				return
			}

			stream <- data
		}
	}()
	return stream
}

func (p *Peer) routeRequest(ctx Context, rw *ResponseWriter, data []byte) {
	p.rqMu.Lock()
	handler, ok := p.requestHandlers[ctx.Scope()]
	p.rqMu.Unlock()
	if ok {
		handler(ctx, data, rw)
	}
}

func (p *Peer) routeStream(ctx Context, rw *ResponseWriter, data chan []byte) {
	p.stMu.Lock()
	handler, ok := p.streamHandlers[ctx.Scope()]
	p.stMu.Unlock()
	if ok {
		go handler(ctx, data, rw)
	}
}

func (p *Peer) AddRequestHandler(scope string, handler RequestHandlerFunc) {
	p.rqMu.Lock()
	p.requestHandlers[scope] = handler
	p.rqMu.Unlock()
}

func (p *Peer) AddStreamHandler(scope string, handler StreamHandlerFunc) {
	p.stMu.Lock()
	p.streamHandlers[scope] = handler
	p.stMu.Unlock()
}
