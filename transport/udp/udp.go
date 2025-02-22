package udp

import (
	"errors"
	"net"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

// It is advised to define a constant (max) size for all relevant byte buffers, e.g:
// const bufSize = 65000
const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return &Socket{
		conn:     conn,
		address:  conn.LocalAddr().String(),
		ins:      []transport.Packet{},
		outs:     []transport.Packet{},
		isClosed: false,

		sendMutex:  &sync.Mutex{},
		recvMutex:  &sync.Mutex{},
		closeMutex: &sync.Mutex{},
		insMutex:   &sync.RWMutex{},
		outsMutex:  &sync.RWMutex{},
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	conn     *net.UDPConn
	address  string
	ins      []transport.Packet
	outs     []transport.Packet
	isClosed bool

	sendMutex  *sync.Mutex
	recvMutex  *sync.Mutex
	closeMutex *sync.Mutex
	insMutex   *sync.RWMutex
	outsMutex  *sync.RWMutex
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()

	if s.isClosed {
		return errors.New("socket is already closed")
	}
	s.isClosed = true
	return s.conn.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	s.sendMutex.Lock()
	defer s.sendMutex.Unlock()

	s.closeMutex.Lock()
	if s.isClosed {
		s.closeMutex.Unlock()
		return errors.New("cannot send on a closed socket")
	}
	s.closeMutex.Unlock()

	data, err := pkt.Marshal()
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return err
	}

	if timeout > 0 {
		if err := s.conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	} else {
		if err := s.conn.SetWriteDeadline(time.Time{}); err != nil {
			return err
		}
	}

	_, err = s.conn.WriteToUDP(data, udpAddr)
	if err != nil {
		return err
	}

	s.outsMutex.Lock()
	s.outs = append(s.outs, pkt)
	s.outsMutex.Unlock()

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	s.recvMutex.Lock()
	defer s.recvMutex.Unlock()

	s.closeMutex.Lock()
	if s.isClosed {
		s.closeMutex.Unlock()
		return transport.Packet{}, errors.New("socket is closed")
	}
	s.closeMutex.Unlock()

	buf := make([]byte, bufSize)

	if timeout > 0 {
		if err := s.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return transport.Packet{}, err
		}
	} else {
		if err := s.conn.SetReadDeadline(time.Time{}); err != nil {
			return transport.Packet{}, err
		}
	}

	n, _, err := s.conn.ReadFromUDP(buf)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return transport.Packet{}, transport.TimeoutError(0)
		}
		return transport.Packet{}, err
	}

	var pkt transport.Packet
	err = pkt.Unmarshal(buf[:n])

	if err != nil {
		return transport.Packet{}, err
	}

	s.insMutex.Lock()
	s.ins = append(s.ins, pkt)
	s.insMutex.Unlock()

	return pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.address
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	s.insMutex.RLock()
	defer s.insMutex.RUnlock()

	return s.ins
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	s.outsMutex.RLock()
	defer s.outsMutex.RUnlock()

	return s.outs
}
