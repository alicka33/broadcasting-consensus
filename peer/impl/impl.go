package impl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/peer/impl/paxos"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/exp/rand"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	n := &node{
		conf:             conf,
		routingTable:     make(peer.RoutingTable),
		stopCh:           make(chan struct{}),
		ackChannels:      make(map[string]chan struct{}),
		sequenceNumbers:  make(map[string]uint),
		sentPacket:       make(map[string][]string),
		sentRumor:        make(map[string]map[uint]types.Rumor),
		catalog:          make(peer.Catalog),
		replyCh:          make(map[string]chan *types.DataReplyMessage),
		searchReplyCh:    make(map[string]chan *types.SearchReplyMessage),
		processedReplies: make(map[string]struct{}),
		acceptor:         paxos.NewAcceptor(0),
		proposer:         paxos.NewProposer(0, conf.PaxosID, conf.TotalPeers, conf.PaxosProposerRetry, conf.PaxosThreshold),
		futureMessages:   make(map[uint][]*types.TLCMessage),
	}
	n.SetRoutingEntry(conf.Socket.GetAddress(), conf.Socket.GetAddress())
	return n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf         peer.Configuration
	routingTable peer.RoutingTable
	tableLock    sync.RWMutex
	stopCh       chan struct{}
	wg           sync.WaitGroup

	sequenceNumbers map[string]uint
	seqLock         sync.Mutex

	ackLock     sync.Mutex
	ackChannels map[string]chan struct{}

	sentPacketLock sync.Mutex
	sentPacket     map[string][]string

	sentRumorLock sync.Mutex
	sentRumor     map[string]map[uint]types.Rumor

	catalog   peer.Catalog
	replyCh   map[string]chan *types.DataReplyMessage
	replyLock sync.Mutex

	searchReplyCh   map[string]chan *types.SearchReplyMessage
	searchReplyLock sync.Mutex

	processedReplies     map[string]struct{}
	processedRepliesLock sync.Mutex

	acceptor *paxos.Acceptor
	proposer *paxos.Proposer

	futureMessages map[uint][]*types.TLCMessage
}

// Start implements peer.Service
func (n *node) Start() error {
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.handleChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.handleRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.handleAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.handleStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.handlePrivateMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.handleEmptyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, n.handleDataRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, n.handleDataReplyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, n.handleSearchRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, n.handleSearchReplyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPrepareMessage{}, n.handlePaxosPrepareMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosProposeMessage{}, n.handlePaxosProposeMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPromiseMessage{}, n.handlePaxosPromiseMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosAcceptMessage{}, n.handlePaxosAcceptMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.TLCMessage{}, n.handleTLCMessage)

	n.wg.Add(1)
	go n.listenForMessages()

	n.startHeartbeat()
	n.startAntiEntropy()

	return nil
}

// listenForMessages listens for incoming messages and processes them.
func (n *node) listenForMessages() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		default:
			pkt, err := n.receivePacket()
			if err == nil {
				n.handlePacket(pkt)
			} else if errors.Is(err, transport.TimeoutError(0)) {
				continue
			} else {
				return
			}
		}
	}
}

// receivePacket handles receiving packets with proper error handling.
func (n *node) receivePacket() (*transport.Packet, error) {
	pkt, err := n.conf.Socket.Recv(time.Second * 1)

	if err != nil {
		if errors.Is(err, transport.TimeoutError(0)) {
			return nil, transport.TimeoutError(0)
		}
		return nil, err
	}
	return &pkt, nil
}

// handlePacket processes or relays the packet based on its destination.
func (n *node) handlePacket(pkt *transport.Packet) {
	if pkt == nil {
		return
	}

	if pkt.Header.Destination == n.conf.Socket.GetAddress() {
		err := n.conf.MessageRegistry.ProcessPacket(*pkt)
		if err != nil {
			return
		}
		return
	}
	n.relayPacket(pkt)
}

// relayPacket relays the packet to the appropriate next hop.
func (n *node) relayPacket(pkt *transport.Packet) {
	n.tableLock.RLock()
	nextHop, exists := n.routingTable[pkt.Header.Destination]
	n.tableLock.RUnlock()

	if exists {
		header := pkt.Header.Copy()
		header.RelayedBy = n.conf.Socket.GetAddress()
		newPkt := transport.Packet{
			Header: &header,
			Msg:    pkt.Msg,
		}

		err := n.conf.Socket.Send(nextHop, newPkt, 0)
		if err != nil {
			return
		}
	}
}

// Stop implements peer.Service
func (n *node) Stop() error {
	close(n.stopCh)

	n.wg.Wait()

	if closableSocket, ok := n.conf.Socket.(transport.ClosableSocket); ok {
		if err := closableSocket.Close(); err != nil {
			return fmt.Errorf("error closing socket: %w", err)
		}
	} else {
		return fmt.Errorf("socket does not support closing")
	}

	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	n.tableLock.Lock()
	relay, exists := n.routingTable[dest]
	n.tableLock.Unlock()

	if !exists {
		return fmt.Errorf("unicast failed: unknown destination %s", dest)
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest)
	pkt := transport.Packet{Header: &header, Msg: &msg}

	return n.conf.Socket.Send(relay, pkt, 0)
}

func (n *node) Broadcast(msg transport.Message) error {
	rumor := types.Rumor{
		Origin:   n.conf.Socket.GetAddress(),
		Sequence: n.getNextSequence(n.conf.Socket.GetAddress()),
		Msg:      &msg,
	}

	n.sentRumorLock.Lock()
	if _, exists := n.sentRumor[rumor.Origin]; !exists {
		n.sentRumor[rumor.Origin] = make(map[uint]types.Rumor)
	}
	n.sentRumor[rumor.Origin][rumor.Sequence] = rumor
	n.sentRumorLock.Unlock()

	rumorsMessage := types.RumorsMessage{
		Rumors: []types.Rumor{rumor},
	}

	notInclude := []string{}
	rumorsMessagePacketID := n.sendRumorToRandomNeighbor(rumorsMessage, notInclude)
	go n.waitForAck(rumorsMessagePacketID, rumorsMessage)

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress())
	packet := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	if err := n.conf.MessageRegistry.ProcessPacket(packet); err != nil {
		return fmt.Errorf("failed to process packet locally: %w", err)
	}
	return nil
}

func (n *node) sendRumorToRandomNeighbor(rumorMsg types.RumorsMessage, neighborsNotInclude []string) string {
	neighbors := n.getNeighbors()
	filteredNeighbors := n.filterNeighbors(neighbors, neighborsNotInclude)

	if len(filteredNeighbors) > 0 {
		randomNeighbor := filteredNeighbors[rand.Intn(len(filteredNeighbors))]

		rumorsTransportMessage, _ := n.conf.MessageRegistry.MarshalMessage(&rumorMsg)
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), randomNeighbor)
		packet := transport.Packet{
			Header: &header,
			Msg:    &rumorsTransportMessage,
		}

		err := n.conf.Socket.Send(randomNeighbor, packet, 0)
		if err != nil {
			return ""
		}

		n.sentPacketLock.Lock()
		n.sentPacket[packet.Header.PacketID] = append(n.sentPacket[packet.Header.PacketID], randomNeighbor)
		n.sentPacketLock.Unlock()

		return packet.Header.PacketID
	}
	return ""
}

func (n *node) getNeighbors() []string {
	n.tableLock.RLock()
	defer n.tableLock.RUnlock()

	neighbors := make([]string, 0, len(n.routingTable))
	for origin, relay := range n.routingTable {
		if origin == n.conf.Socket.GetAddress() || origin != relay {
			continue
		}

		neighbors = append(neighbors, origin)
	}

	return neighbors
}

func (n *node) filterNeighbors(neighbors []string, neighborsNotInclude []string) []string {
	filteredNeighbors := []string{}
	for _, neighbor := range neighbors {
		exclude := false
		for _, excludeNeighbor := range neighborsNotInclude {
			if neighbor == excludeNeighbor {
				exclude = true
				break
			}
		}
		if !exclude {
			filteredNeighbors = append(filteredNeighbors, neighbor)
		}
	}
	return filteredNeighbors
}

func (n *node) waitForAck(packetID string, rumorsMessage types.RumorsMessage) {
	ackTimeout := n.conf.AckTimeout
	if ackTimeout == 0 {
		return
	}

	timer := time.NewTimer(ackTimeout)
	ackCh := n.getAckChannel(packetID)

	select {
	case <-ackCh:
		timer.Stop()
	case <-timer.C:
		n.notifyAckReceived(packetID)

		n.sentPacketLock.Lock()
		neighboursNotInclude := []string{}
		if len(n.sentPacket[packetID]) > 0 {
			lastNeighbour := n.sentPacket[packetID][len(n.sentPacket[packetID])-1]
			neighboursNotInclude = append(neighboursNotInclude, lastNeighbour)
		}
		n.sentPacketLock.Unlock()

		n.sendRumorToRandomNeighbor(rumorsMessage, neighboursNotInclude)
		go n.waitForAck(packetID, rumorsMessage)
	}
}

func (n *node) getAckChannel(packetID string) chan struct{} {
	n.ackLock.Lock()
	defer n.ackLock.Unlock()

	if ch, exists := n.ackChannels[packetID]; exists {
		return ch
	}

	ch := make(chan struct{})
	n.ackChannels[packetID] = ch
	return ch
}

func (n *node) notifyAckReceived(packetID string) {
	n.ackLock.Lock()
	defer n.ackLock.Unlock()

	if ch, exists := n.ackChannels[packetID]; exists {
		close(ch)
		delete(n.ackChannels, packetID)
	}
}

func (n *node) startHeartbeat() {
	if n.conf.HeartbeatInterval == 0 {
		return
	}

	emptyMsg := types.EmptyMessage{}
	emptyTransportMsg, _ := n.conf.MessageRegistry.MarshalMessage(&emptyMsg)
	err := n.Broadcast(emptyTransportMsg)
	if err != nil {
		return
	}

	ticker := time.NewTicker(n.conf.HeartbeatInterval)
	n.wg.Add(1)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-n.stopCh:
				n.wg.Done()
				return
			case <-ticker.C:
				emptyMsg := types.EmptyMessage{}
				emptyTransportMsg, _ := n.conf.MessageRegistry.MarshalMessage(&emptyMsg)
				err := n.Broadcast(emptyTransportMsg)
				if err != nil {
					return
				}
			}
		}
	}()
}

func (n *node) startAntiEntropy() {
	if n.conf.AntiEntropyInterval == 0 {
		return
	}

	statusMsg := n.createStatusMessage()
	n.sendStatusToRandomNeighbor(statusMsg)

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			select {
			case <-n.stopCh:
				return
			case <-time.After(n.conf.AntiEntropyInterval):
				statusMsg := n.createStatusMessage()
				n.sendStatusToRandomNeighbor(statusMsg)
			}
		}
	}()
}

func (n *node) createStatusMessage() types.StatusMessage {
	status := types.StatusMessage{}

	n.seqLock.Lock()
	defer n.seqLock.Unlock()

	for peer, seq := range n.sequenceNumbers {
		status[peer] = seq
	}

	return status
}

func (n *node) updateSequenceNumber(origin string, sequence uint) {
	n.seqLock.Lock()
	defer n.seqLock.Unlock()
	n.sequenceNumbers[origin] = sequence
}

func (n *node) getNextSequence(peer string) uint {
	n.seqLock.Lock()
	defer n.seqLock.Unlock()

	if _, exists := n.sequenceNumbers[peer]; !exists {
		n.sequenceNumbers[peer] = 0
	}

	n.sequenceNumbers[peer]++
	return n.sequenceNumbers[peer]
}

func (n *node) accessSequence(peer string) uint {
	n.seqLock.Lock()
	defer n.seqLock.Unlock()

	if _, exists := n.sequenceNumbers[peer]; !exists {
		n.sequenceNumbers[peer] = 0
	}

	return n.sequenceNumbers[peer]
}

func (n *node) sendStatusToRandomNeighbor(status types.StatusMessage) {
	statusTransportMessage, _ := n.conf.MessageRegistry.MarshalMessage(&status)

	neighbors := n.getNeighbors()

	if len(neighbors) > 0 {
		randomNeighbor := neighbors[rand.Intn(len(neighbors))]
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), randomNeighbor)
		packet := transport.Packet{
			Header: &header,
			Msg:    &statusTransportMessage,
		}
		_ = n.conf.Socket.Send(randomNeighbor, packet, 0)
	}
}

// AddPeer implements peer.Messaging
func (n *node) AddPeer(addr ...string) {
	n.tableLock.Lock()
	defer n.tableLock.Unlock()

	for _, a := range addr {
		if a == n.conf.Socket.GetAddress() {
			continue
		}

		_, exists := n.routingTable[a]
		if !exists {
			n.SetRoutingEntry(a, a)
		}
	}
}

// GetRoutingTable implements peer.Messaging
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.tableLock.RLock()
	defer n.tableLock.RUnlock()

	tableCopy := make(peer.RoutingTable)
	for origin, relay := range n.routingTable {
		tableCopy[origin] = relay
	}
	return tableCopy
}

// SetRoutingEntry implements peer.Messaging
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		delete(n.routingTable, origin)
	} else {
		n.routingTable[origin] = relayAddr
	}
}

func (n *node) originIsNeighbor(origin string) bool {
	relay, ok := n.routingTable[origin]
	if !ok {
		return false
	}
	return origin == relay
}

func (n *node) updateRoutingTable(rumor types.Rumor, pkt transport.Packet) {
	n.tableLock.Lock()
	if !n.originIsNeighbor(rumor.Origin) {
		n.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)
	}
	n.tableLock.Unlock()
}

func (n *node) handleChatMessage(msg types.Message, pkt transport.Packet) error {
	_, ok := msg.(*types.ChatMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}
	return nil
}

func (n *node) handleRumorsMessage(msg types.Message, pkt transport.Packet) error {
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	expectedRumorFound := false

	for _, rumor := range rumorsMsg.Rumors {
		expectedSeq := n.accessSequence(rumor.Origin) + 1

		if rumor.Sequence == expectedSeq {

			n.updateRoutingTable(rumor, pkt)
			n.updateSequenceNumber(rumor.Origin, rumor.Sequence)

			newPkt := transport.Packet{
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}

			if err := n.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
				return fmt.Errorf("failed to process packet in rumor handler: %w", err)
			}

			n.sentRumorLock.Lock()
			if _, exists := n.sentRumor[rumor.Origin]; !exists {
				n.sentRumor[rumor.Origin] = make(map[uint]types.Rumor)
			}
			n.sentRumor[rumor.Origin][rumor.Sequence] = rumor
			n.sentRumorLock.Unlock()

			expectedRumorFound = true
		}
	}

	status := n.createStatusMessage()

	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        status,
	}

	ackTransportMessage, _ := n.conf.MessageRegistry.MarshalMessage(&ack)
	ackHeader := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source)
	ackPacket := transport.Packet{
		Header: &ackHeader,
		Msg:    &ackTransportMessage,
	}
	ackPacket.Header.PacketID = pkt.Header.PacketID

	if err := n.conf.Socket.Send(pkt.Header.Source, ackPacket, 0); err != nil {
		return fmt.Errorf("failed to process ack in rumor handler: %w", err)
	}

	if expectedRumorFound {
		neighborsToExclude := []string{pkt.Header.Source}
		n.sendRumorToRandomNeighbor(*rumorsMsg, neighborsToExclude)
	}

	return nil
}

func (n *node) handleAckMessage(msg types.Message, pkt transport.Packet) error {
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	packetID := ackMsg.AckedPacketID
	n.notifyAckReceived(packetID)

	statusMsg := ackMsg.Status
	statusTransportMessage, _ := n.conf.MessageRegistry.MarshalMessage(&statusMsg)

	statusPacket := transport.Packet{
		Header: pkt.Header,
		Msg:    &statusTransportMessage,
	}

	if err := n.conf.MessageRegistry.ProcessPacket(statusPacket); err != nil {
		return fmt.Errorf("failed to process status message in ack: %w", err)
	}

	return nil
}

func (n *node) handleStatusMessage(msg types.Message, pkt transport.Packet) error {
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	remoteStatus := *statusMsg
	localStatus := n.createStatusMessage()

	hasNewRumors := false
	needsRumors := false

	var missingRumors []types.Rumor

	for origin, remoteSeq := range remoteStatus {
		localSeq, exists := localStatus[origin]
		if !exists || localSeq < remoteSeq {
			hasNewRumors = true
		} else if localSeq > remoteSeq {
			needsRumors = true
			for seqNum := remoteSeq + 1; seqNum <= localSeq; seqNum++ {
				missingRumors = append(missingRumors, n.getRumorBySequence(origin, seqNum))
			}
		}
	}
	for origin, localSeq := range localStatus {
		_, exists := remoteStatus[origin]
		if !exists {
			needsRumors = true
			for seqNum := uint(1); seqNum <= localSeq; seqNum++ {
				missingRumors = append(missingRumors, n.getRumorBySequence(origin, seqNum))
			}
		}
	}

	if hasNewRumors && needsRumors {
		n.sendStatusToPeer(localStatus, pkt.Header.Source)
		n.sendMissingRumors(missingRumors, pkt.Header.Source)
		return nil
	}

	if hasNewRumors {
		n.sendStatusToPeer(localStatus, pkt.Header.Source)
		return nil
	}

	if needsRumors {
		n.sendMissingRumors(missingRumors, pkt.Header.Source)
		return nil
	}

	n.maybeContinueMongering(localStatus, pkt.Header.Source)
	return nil
}

func (n *node) sendStatusToPeer(status types.StatusMessage, peerAddr string) {
	statusTransportMessage, _ := n.conf.MessageRegistry.MarshalMessage(&status)

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), peerAddr)
	packet := transport.Packet{
		Header: &header,
		Msg:    &statusTransportMessage,
	}
	_ = n.conf.Socket.Send(peerAddr, packet, 0)
}

func (n *node) sendMissingRumors(rumors []types.Rumor, peerAddr string) {
	if len(rumors) == 0 {
		return
	}
	rumorsMessage := types.RumorsMessage{Rumors: rumors}
	rumorsTransportMessage, _ := n.conf.MessageRegistry.MarshalMessage(&rumorsMessage)

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), peerAddr)
	packet := transport.Packet{
		Header: &header,
		Msg:    &rumorsTransportMessage,
	}
	_ = n.conf.Socket.Send(peerAddr, packet, 0)
}

func (n *node) maybeContinueMongering(status types.StatusMessage, excludePeer string) {
	if rand.Float64() < n.conf.ContinueMongering {
		neighbors := n.getNeighbors()
		filteredNeighbors := []string{}
		for _, neighbor := range neighbors {
			if neighbor != excludePeer {
				filteredNeighbors = append(filteredNeighbors, neighbor)
			}
		}
		if len(filteredNeighbors) > 0 {
			randomNeighbor := filteredNeighbors[rand.Intn(len(filteredNeighbors))]
			n.sendStatusToPeer(status, randomNeighbor)
		}
	}
}

func (n *node) getRumorBySequence(origin string, seq uint) types.Rumor {
	n.sentRumorLock.Lock()
	defer n.sentRumorLock.Unlock()
	return n.sentRumor[origin][seq]
}

func (n *node) handlePrivateMessage(msg types.Message, pkt transport.Packet) error {
	privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	recipientFound := false
	for recipient := range privateMsg.Recipients {
		if recipient == n.conf.Socket.GetAddress() {
			recipientFound = true
			break
		}
	}

	if recipientFound {
		privatePacket := transport.Packet{
			Header: pkt.Header,
			Msg:    privateMsg.Msg,
		}
		err := n.conf.MessageRegistry.ProcessPacket(privatePacket)
		if err != nil {
			return fmt.Errorf("processing private message failed")
		}
		return nil
	}

	return nil
}

func (n *node) handleEmptyMessage(msg types.Message, pkt transport.Packet) error {
	_, ok := msg.(*types.EmptyMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}
	return nil
}

// HW2 begin

func (n *node) Upload(data io.Reader) (metahash string, err error) {
	blobStore := n.conf.Storage.GetDataBlobStore()
	var chunkHashes []string
	buf := make([]byte, n.conf.ChunkSize)

	for {
		r, err := data.Read(buf)
		if err != nil && err != io.EOF {
			return "", err
		}
		if r == 0 {
			break
		}

		chunk := buf[:r]
		chunkHash := sha256.Sum256(chunk)
		chunkHashHex := hex.EncodeToString(chunkHash[:])

		blobStore.Set(chunkHashHex, chunk)
		chunkHashes = append(chunkHashes, chunkHashHex)

		if err == io.EOF {
			break
		}
	}

	metafileValue := strings.Join(chunkHashes, peer.MetafileSep)

	metafileHash := sha256.Sum256([]byte(metafileValue))
	metafileHashHex := hex.EncodeToString(metafileHash[:])

	blobStore.Set(metafileHashHex, []byte(metafileValue))

	return metafileHashHex, nil
}

func (n *node) UpdateCatalog(key string, peerAddr string) {
	if n.catalog == nil {
		n.catalog = make(peer.Catalog)
	}
	if _, exists := n.catalog[key]; !exists {
		n.catalog[key] = make(map[string]struct{})
	}
	n.catalog[key][peerAddr] = struct{}{}
}

func (n *node) GetCatalog() peer.Catalog {
	return n.catalog
}

func (n *node) RemoveFromCatalog(key string, peerAddr string) {
	if n.catalog == nil {
		return
	}
	if peers, exists := n.catalog[key]; exists {
		delete(peers, peerAddr)
		if len(peers) == 0 {
			delete(n.catalog, key)
		}
	}
}

func (n *node) Download(metahash string) ([]byte, error) {
	metafileData, err := n.retrieveMetafile(metahash)
	if err != nil {
		return nil, err
	}

	chunkHashes := n.extractChunkHashes(metafileData)
	chunks, err := n.downloadChunks(chunkHashes)
	if err != nil {
		return nil, err
	}

	return n.reconstructData(chunks), nil
}

func (n *node) retrieveMetafile(metahash string) ([]byte, error) {
	metafileData := n.conf.Storage.GetDataBlobStore().Get(metahash)
	if metafileData != nil {
		return metafileData, nil
	}

	randomPeer, err := n.getPeerForMetafile(metahash)
	if err != nil {
		return nil, err
	}

	reqID := generateRequestID()
	dataRequest := types.DataRequestMessage{RequestID: reqID, Key: metahash}
	reply, err := n.requestData(randomPeer, reqID, dataRequest)
	if err != nil {
		return nil, err
	}
	if reply.Value == nil {
		n.UpdateCatalog(metahash, randomPeer)
		return nil, errors.New("metafile not found on peer")
	}

	n.conf.Storage.GetDataBlobStore().Set(metahash, reply.Value)
	return reply.Value, nil
}

func (n *node) downloadChunks(chunkHashes []string) ([][]byte, error) {
	var chunks [][]byte
	for _, chunkHash := range chunkHashes {
		chunk, err := n.retrieveChunk(chunkHash)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func (n *node) retrieveChunk(chunkHash string) ([]byte, error) {
	chunkData := n.conf.Storage.GetDataBlobStore().Get(chunkHash)
	if chunkData != nil {
		return chunkData, nil
	}

	randomPeer, err := n.getPeerForChunk(chunkHash)
	if err != nil {
		return nil, err
	}

	reqID := generateRequestID()
	dataRequest := types.DataRequestMessage{RequestID: reqID, Key: chunkHash}
	reply, err := n.requestData(randomPeer, reqID, dataRequest)
	if err != nil || reply.Value == nil {
		return nil, errors.New("chunk not found on peer")
	}

	if !n.validateChunk(chunkHash, reply.Value) {
		n.RemoveFromCatalog(chunkHash, randomPeer)
		return nil, errors.New("received invalid chunk")
	}
	n.conf.Storage.GetDataBlobStore().Set(chunkHash, reply.Value)

	return reply.Value, nil
}

func (n *node) requestData(peer string, reqID string, request types.DataRequestMessage,
) (*types.DataReplyMessage, error) {
	replyCh := make(chan *types.DataReplyMessage)
	n.registerReplyChannel(reqID, replyCh)
	defer n.unregisterReplyChannel(reqID)

	err := n.sendDataRequest(peer, request)
	if err != nil {
		return nil, err
	}

	return n.waitForReplyWithBackoff(reqID, peer, request)
}

func (n *node) getPeerForMetafile(metahash string) (string, error) {
	randomPeer := n.getRandomPeerFromCatalog(metahash)
	if randomPeer == "" {
		return "", errors.New("no peers available to fetch metafile")
	}
	if !n.isPeerInRoutingTable(randomPeer) {
		return "", errors.New("peer is not in routing table")
	}
	return randomPeer, nil
}

func (n *node) getPeerForChunk(chunkHash string) (string, error) {
	randomPeer := n.getRandomPeerFromCatalog(chunkHash)
	if randomPeer == "" {
		return "", errors.New("no catalog entry for chunk and not found locally")
	}
	if !n.isPeerInRoutingTable(randomPeer) {
		return "", errors.New("peer is not in routing table")
	}
	return randomPeer, nil
}

func (n *node) isPeerInRoutingTable(peerAddr string) bool {
	_, exists := n.routingTable[peerAddr]
	return exists
}

func generateRequestID() string {
	return xid.New().String()
}

func (n *node) sendDataRequest(peerAddr string, request types.DataRequestMessage) error {
	requestMsg, err := n.conf.MessageRegistry.MarshalMessage(&request)
	if err != nil {
		return fmt.Errorf("failed to marshal DataRequestMessage: %w", err)
	}
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), peerAddr)
	packet := transport.Packet{
		Header: &header,
		Msg:    &requestMsg,
	}

	err = n.conf.Socket.Send(peerAddr, packet, 0)
	if err != nil {
		return fmt.Errorf("failed to send data request packet: %w", err)
	}
	return nil
}

func (n *node) waitForReplyWithBackoff(requestID string, peerAddr string, dataRequest types.DataRequestMessage,
) (*types.DataReplyMessage, error) {
	backoff := n.conf.BackoffDataRequest
	backoffDuration := backoff.Initial
	retries := backoff.Retry

	replyCh := n.getReplyChannel(requestID)

	for i := uint(0); i < retries; i++ {
		select {
		case reply := <-replyCh:
			if reply == nil || reply.Value == nil {
				return nil, fmt.Errorf("received invalid or empty reply for request %s", requestID)
			}

			n.processedRepliesLock.Lock()
			if _, seen := n.processedReplies[reply.RequestID]; seen {
				n.processedRepliesLock.Unlock()
				continue
			}

			n.processedReplies[reply.RequestID] = struct{}{}
			n.processedRepliesLock.Unlock()

			return reply, nil

		case <-time.After(backoffDuration):
			if i+1 == retries {
				return nil, fmt.Errorf("max retries reached, no reply for request %s", requestID)
			}

			backoffDuration *= time.Duration(backoff.Factor)
			err := n.sendDataRequest(peerAddr, dataRequest)
			if err != nil {
				return nil, fmt.Errorf("retrying data request to %s failed: %w", peerAddr, err)
			}
		}
	}
	return nil, fmt.Errorf("timeout waiting for reply to request %s", requestID)
}

func (n *node) registerReplyChannel(requestID string, ch chan *types.DataReplyMessage) {
	n.replyLock.Lock()
	defer n.replyLock.Unlock()

	n.replyCh[requestID] = ch
}

func (n *node) unregisterReplyChannel(requestID string) {
	n.replyLock.Lock()
	defer n.replyLock.Unlock()

	delete(n.replyCh, requestID)
}

func (n *node) getReplyChannel(requestID string) chan *types.DataReplyMessage {
	n.replyLock.Lock()
	defer n.replyLock.Unlock()

	return n.replyCh[requestID]
}

func (n *node) extractChunkHashes(metafileData []byte) []string {
	lines := bytes.Split(metafileData, []byte{'\n'})
	hashes := make([]string, 0, len(lines))
	for _, line := range lines {
		hashes = append(hashes, string(line))
	}
	return hashes
}

func (n *node) validateChunk(expectedHash string, chunk []byte) bool {
	h := sha256.New()
	h.Write(chunk)
	computedHash := hex.EncodeToString(h.Sum(nil))
	return computedHash == expectedHash
}

func (n *node) getRandomPeerFromCatalog(key string) string {
	peers, exists := n.catalog[key]
	if !exists || len(peers) == 0 {
		return ""
	}
	var peerList []string
	for peerAddr := range peers {
		peerList = append(peerList, peerAddr)
	}
	return peerList[rand.Intn(len(peerList))]
}

func (n *node) reconstructData(chunks [][]byte) []byte {
	var buffer bytes.Buffer
	for _, chunk := range chunks {
		buffer.Write(chunk)
	}
	return buffer.Bytes()
}

func (n *node) handleDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	requestMsg, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	if requestMsg.RequestID == "" || requestMsg.Key == "" {
		return fmt.Errorf("invalid DataRequestMessage from %s: %+v", pkt.Header.Source, msg)
	}

	n.processedRepliesLock.Lock()
	if _, processed := n.processedReplies[requestMsg.RequestID]; processed {
		n.processedRepliesLock.Unlock()
		return nil
	}

	n.processedReplies[requestMsg.RequestID] = struct{}{}
	n.processedRepliesLock.Unlock()

	value := n.conf.Storage.GetDataBlobStore().Get(requestMsg.Key)

	if !n.isPeerInRoutingTable(pkt.Header.Source) {
		return errors.New("peer is not in routing table")
	}

	reply := types.DataReplyMessage{
		RequestID: requestMsg.RequestID,
		Key:       requestMsg.Key,
		Value:     value,
	}

	replyMsg, err := n.conf.MessageRegistry.MarshalMessage(&reply)
	if err != nil {
		return fmt.Errorf("failed to marshal DataReplyMessage: %w", err)
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source)
	packet := transport.Packet{
		Header: &header,
		Msg:    &replyMsg,
	}

	err = n.conf.Socket.Send(pkt.Header.Source, packet, 0)
	if err != nil {
		return fmt.Errorf("failed to send data request: %w", err)
	}
	return nil
}

func (n *node) handleDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	replyMsg, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	replyCh := n.getReplyChannel(replyMsg.RequestID)

	if replyCh != nil {
		replyCh <- replyMsg
	}
	return nil
}

func (n *node) Tag(name string, mh string) error {
	namingStore := n.conf.Storage.GetNamingStore()
	if n.conf.TotalPeers <= 1 {
		namingStore.Set(name, []byte(mh))
		return nil
	}

	existingMetahash := namingStore.Get(name)
	if existingMetahash != nil {
		return fmt.Errorf("name '%s' is already tagged to a metahash", name)
	}

	for {
		paxosID := n.proposer.GetCurrentID()
		prepareMsg := &types.PaxosPrepareMessage{
			Step:   n.proposer.GetStep(),
			Source: n.conf.Socket.GetAddress(),
			ID:     paxosID,
		}
		prepareMsgTransport, _ := n.conf.MessageRegistry.MarshalMessage(prepareMsg)
		if err := n.Broadcast(prepareMsgTransport); err != nil {
			return fmt.Errorf("error broadcasting prepare message: %w", err)
		}

		select {
		case <-n.proposer.PromiseCh:
			break
		case <-time.After(n.proposer.RetryTimeout):
			n.proposer.IncrementID()
			continue
		}

		promises := n.proposer.GetPromises()

		var proposeValue *types.PaxosValue
		highestAcceptedID := uint(0)
		for _, promise := range promises {
			if promise.AcceptedValue != nil && promise.AcceptedID > highestAcceptedID {
				highestAcceptedID = promise.AcceptedID
				proposeValue = promise.AcceptedValue
			}
		}
		if proposeValue == nil {
			proposeValue = &types.PaxosValue{Filename: name, Metahash: mh}
		}

		proposeMsg := &types.PaxosProposeMessage{
			Step:  n.proposer.GetStep(),
			ID:    paxosID,
			Value: *proposeValue,
		}

		proposeMsgTransport, _ := n.conf.MessageRegistry.MarshalMessage(proposeMsg)
		if err := n.Broadcast(proposeMsgTransport); err != nil {
			return fmt.Errorf("error broadcasting propose message: %w", err)
		}

		select {
		case <-n.proposer.AcceptCh:
			n.proposer.IncrementID()
			n.proposer.ResetPromises()
			n.proposer.ResetAccepts()
			return nil
		case <-time.After(n.proposer.RetryTimeout):
			n.proposer.IncrementID()
			n.proposer.ResetPromises()
			n.proposer.ResetAccepts()
			continue
		}
	}
}

func (n *node) BroadcastTLCMessage(name, mh string) error {
	blockchainStore := n.conf.Storage.GetBlockchainStore()
	lastBlockHash := blockchainStore.Get(storage.LastBlockKey)
	if lastBlockHash == nil {
		lastBlockHash = make([]byte, 32)
	}

	block := &types.BlockchainBlock{
		Index:    n.proposer.GetStep(),
		Value:    types.PaxosValue{Filename: name, Metahash: mh},
		PrevHash: lastBlockHash,
	}
	block.Hash = ComputeBlockHash(block)

	tlcMsg := &types.TLCMessage{
		Step:  n.proposer.GetStep(),
		Block: *block,
	}
	tlcMsgTransport, _ := n.conf.MessageRegistry.MarshalMessage(tlcMsg)

	n.proposer.MarkBroadcasted(int(tlcMsg.Step))
	return n.Broadcast(tlcMsgTransport)
}

func ComputeBlockHash(block *types.BlockchainBlock) []byte {
	hashInput := strconv.Itoa(int(block.Index)) + block.Value.Filename + block.Value.Metahash + string(block.PrevHash)
	hash := sha256.Sum256([]byte(hashInput))
	return hash[:]
}

func (n *node) Resolve(name string) string {
	namingStore := n.conf.Storage.GetNamingStore()

	metahash := namingStore.Get(name)
	if metahash == nil {
		return ""
	}

	return string(metahash)
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	requestID := generateRequestID()

	neighbors := n.getNeighbors()
	if len(neighbors) == 0 {
		return n.GetMatchingNames(&reg), nil
	}

	shuffledNeighbors := shuffle(neighbors)
	budgetDistribution := distributeBudget(budget, len(shuffledNeighbors))

	searchReplyCh := make(chan *types.SearchReplyMessage)
	n.registerSearchReplyChannel(requestID, searchReplyCh)
	defer n.unregisterSearchReplyChannel(requestID)

	for i, neighbor := range shuffledNeighbors {
		searchRequest := types.SearchRequestMessage{
			RequestID: requestID,
			Origin:    n.conf.Socket.GetAddress(),
			Pattern:   reg.String(),
			Budget:    budgetDistribution[i],
		}
		err := n.sendSearchRequest(neighbor, searchRequest)
		if err != nil {
			return []string{}, err
		}
	}

	matchedNamesMap := make(map[string]struct{})
	timeoutCh := time.After(timeout)

	for {
		select {
		case reply := <-searchReplyCh:
			for _, fileInfo := range reply.Responses {
				if _, exists := matchedNamesMap[fileInfo.Name]; !exists {
					matchedNamesMap[fileInfo.Name] = struct{}{}
				}
			}
		case <-timeoutCh:
			localMatches := n.GetMatchingNames(&reg)

			for _, name := range localMatches {
				matchedNamesMap[name] = struct{}{}
			}

			matchedNames := make([]string, 0, len(matchedNamesMap))
			for name := range matchedNamesMap {
				matchedNames = append(matchedNames, name)
			}
			return matchedNames, nil
		}
	}
}

func (n *node) updateNamingStoreAndCatalog(fileInfo types.FileInfo, peerAddres string) {
	existingMetahash := n.conf.Storage.GetNamingStore().Get(fileInfo.Name)
	if existingMetahash == nil || !bytes.Equal(existingMetahash, []byte(fileInfo.Metahash)) {
		n.conf.Storage.GetNamingStore().Set(fileInfo.Name, []byte(fileInfo.Metahash))
	}

	for _, chunk := range fileInfo.Chunks {
		if chunk != nil {
			n.UpdateCatalog(string(chunk), peerAddres)
		}
	}
}

func (n *node) GetMatchingNames(reg *regexp.Regexp) []string {
	var matchedNames []string

	n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
		if reg.MatchString(key) {
			matchedNames = append(matchedNames, key)
		}
		return true
	})

	return matchedNames
}

func (n *node) registerSearchReplyChannel(requestID string, ch chan *types.SearchReplyMessage) {
	n.searchReplyLock.Lock()
	defer n.searchReplyLock.Unlock()
	n.searchReplyCh[requestID] = ch
}

func (n *node) unregisterSearchReplyChannel(requestID string) {
	n.searchReplyLock.Lock()
	defer n.searchReplyLock.Unlock()
	delete(n.searchReplyCh, requestID)
}

func (n *node) getSearchReplyChannel(requestID string) chan *types.SearchReplyMessage {
	n.searchReplyLock.Lock()
	defer n.searchReplyLock.Unlock()
	return n.searchReplyCh[requestID]
}

func (n *node) sendSearchRequest(peerAddr string, request types.SearchRequestMessage) error {
	requestMsg, err := n.conf.MessageRegistry.MarshalMessage(&request)
	if err != nil {
		return fmt.Errorf("failed to marshal SearchRequestMessage: %w", err)
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), peerAddr)
	packet := transport.Packet{
		Header: &header,
		Msg:    &requestMsg,
	}

	err = n.conf.Socket.Send(peerAddr, packet, 0)
	if err != nil {
		return fmt.Errorf("failed to send search request packet: %w", err)
	}
	return nil
}

func shuffle(slice []string) []string {
	rand.Seed(uint64(time.Now().UnixNano()))
	rand.Shuffle(len(slice), func(i, j int) { slice[i], slice[j] = slice[j], slice[i] })
	return slice
}

func distributeBudget(totalBudget uint, numPeers int) []uint {
	budget := make([]uint, numPeers)
	for i := range budget {
		budget[i] = totalBudget / uint(numPeers)
	}
	for i := 0; i < int(totalBudget)%numPeers; i++ {
		budget[i]++
	}
	return budget
}

func (n *node) getNeighborsExcept(excludeAddr string) []string {
	neighbors := []string{}

	for _, neighbor := range n.getNeighbors() {
		if neighbor != excludeAddr {
			neighbors = append(neighbors, neighbor)
		}
	}

	return neighbors
}

func (n *node) extractChunksFromMetafile(metafileData []byte) [][]byte {
	var chunks [][]byte

	metafileString := string(metafileData)
	metafileChunks := strings.Split(metafileString, peer.MetafileSep)

	for _, chunk := range metafileChunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}

		chunkData := n.conf.Storage.GetDataBlobStore().Get(chunk)
		if chunkData == nil {
			chunks = append(chunks, nil)
		} else {
			chunks = append(chunks, []byte(chunk))
		}
	}

	return chunks
}

func (n *node) handleSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	searchRequestMsg, err := n.validateAndDistributeBudget(msg, pkt)
	if err != nil {
		return err
	}

	matchingFileInfos, err := n.getMatchingFileInfos(searchRequestMsg.Pattern)
	if err != nil {
		return err
	}

	return n.sendSearchReply(searchRequestMsg, matchingFileInfos, pkt.Header.Source)
}

func (n *node) validateAndDistributeBudget(msg types.Message, pkt transport.Packet,
) (*types.SearchRequestMessage, error) {
	searchRequestMsg, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected message type")
	}

	if searchRequestMsg.Budget > 1 {
		neighbors := shuffle(n.getNeighborsExcept(pkt.Header.RelayedBy))
		budgetRemaining := searchRequestMsg.Budget - 1
		if err := n.forwardSearchRequestToNeighbors(neighbors, searchRequestMsg, int(budgetRemaining)); err != nil {
			return nil, err
		}
	}
	return searchRequestMsg, nil
}

func (n *node) forwardSearchRequestToNeighbors(neighbors []string,
	searchRequestMsg *types.SearchRequestMessage, budgetRemaining int) error {
	numNeighbors := len(neighbors)
	if numNeighbors == 0 {
		return nil
	}

	budgetDistribution := distributeBudget(uint(budgetRemaining), numNeighbors)
	for i, neighbor := range neighbors {
		allocatedBudget := budgetDistribution[i]
		if allocatedBudget > 0 {
			forwardedMsg := &types.SearchRequestMessage{
				RequestID: searchRequestMsg.RequestID,
				Origin:    searchRequestMsg.Origin,
				Pattern:   searchRequestMsg.Pattern,
				Budget:    allocatedBudget,
			}
			if err := n.sendSearchRequest(neighbor, *forwardedMsg); err != nil {
				return fmt.Errorf("failed to send search request to neighbor: %w", err)
			}
		}
	}
	return nil
}

func (n *node) getMatchingFileInfos(patternStr string) ([]types.FileInfo, error) {
	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("failed to compile pattern: %w", err)
	}

	var matchingFileInfos []types.FileInfo
	matchingNames := n.GetMatchingNames(pattern)
	for _, name := range matchingNames {
		if fileInfo := n.getFileInfo(name); fileInfo != nil {
			matchingFileInfos = append(matchingFileInfos, *fileInfo)
		}
	}
	return matchingFileInfos, nil
}

func (n *node) getFileInfo(name string) *types.FileInfo {
	metahash := n.conf.Storage.GetNamingStore().Get(name)
	if metahash == nil {
		return nil
	}
	metafileData := n.conf.Storage.GetDataBlobStore().Get(string(metahash))
	if metafileData == nil {
		return nil
	}
	return &types.FileInfo{
		Name:     name,
		Metahash: string(metahash),
		Chunks:   n.extractChunksFromMetafile(metafileData),
	}
}

func (n *node) sendSearchReply(searchRequestMsg *types.SearchRequestMessage,
	matchingFileInfos []types.FileInfo, source string) error {
	reply := &types.SearchReplyMessage{
		RequestID: searchRequestMsg.RequestID,
		Responses: matchingFileInfos,
	}

	replyHeader := transport.NewHeader(
		n.conf.Socket.GetAddress(),
		n.conf.Socket.GetAddress(),
		searchRequestMsg.Origin,
	)
	replyMsgTransport, err := n.conf.MessageRegistry.MarshalMessage(reply)
	if err != nil {
		return fmt.Errorf("failed to marshal SearchReplyMessage: %w", err)
	}

	replyPacket := transport.Packet{
		Header: &replyHeader,
		Msg:    &replyMsgTransport,
	}
	return n.conf.Socket.Send(source, replyPacket, 0)
}

func (n *node) handleSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	replyMsg, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	for _, fileInfo := range replyMsg.Responses {
		n.UpdateCatalog(fileInfo.Metahash, pkt.Header.Source)
		n.updateNamingStoreAndCatalog(fileInfo, pkt.Header.Source)
	}

	if replyCh := n.getSearchReplyChannel(replyMsg.RequestID); replyCh != nil {
		replyCh <- replyMsg
	}
	return nil
}

func (n *node) SearchFirst(pattern regexp.Regexp, expandingRing peer.ExpandingRing) (string, error) {
	localMatchingNames := n.GetMatchingNames(&pattern)

	for _, name := range localMatchingNames {
		metahash := n.conf.Storage.GetNamingStore().Get(name)
		if metahash != nil {
			metafileData := n.conf.Storage.GetDataBlobStore().Get(string(metahash))
			if metafileData != nil {
				if n.hasAllChunks(metafileData) {
					return name, nil
				}
			}
		}
	}

	if len(n.getNeighbors()) == 0 {
		return "", nil
	}

	return n.expandSearchToNeighbors(&pattern, expandingRing)
}

func (n *node) expandSearchToNeighbors(pattern *regexp.Regexp, expandingRing peer.ExpandingRing) (string, error) {
	budget := expandingRing.Initial
	requestID := generateRequestID()
	replyCh := make(chan *types.SearchReplyMessage)

	n.registerSearchReplyChannel(requestID, replyCh)
	defer n.unregisterSearchReplyChannel(requestID)

	for attempt := uint(0); attempt < expandingRing.Retry; attempt++ {
		neighbors := n.getNeighbors()
		if len(neighbors) == 0 {
			break
		}

		budgetDistribution := distributeBudget(budget, len(neighbors))

		for i, neighbor := range neighbors {
			searchRequest := types.SearchRequestMessage{
				RequestID: requestID,
				Origin:    n.conf.Socket.GetAddress(),
				Pattern:   pattern.String(),
				Budget:    budgetDistribution[i],
			}

			if err := n.sendSearchRequest(neighbor, searchRequest); err != nil {
				return "", err
			}
		}

		foundFile := n.waitForReplies(replyCh, expandingRing.Timeout)
		if foundFile != "" {
			return foundFile, nil
		}

		budget *= expandingRing.Factor
	}

	return "", nil
}

func (n *node) waitForReplies(replyCh chan *types.SearchReplyMessage, timeout time.Duration) string {
	timeoutCh := time.After(timeout)

	for {
		select {
		case reply := <-replyCh:
			for _, fileInfo := range reply.Responses {
				if n.allChunksKnown(fileInfo) {
					return fileInfo.Name
				}
			}
		case <-timeoutCh:
			return ""
		}
	}
}

func (n *node) hasAllChunks(metafileData []byte) bool {
	chunkHashes := n.extractChunkHashes(metafileData)

	for _, chunkHash := range chunkHashes {
		chunkData := n.conf.Storage.GetDataBlobStore().Get(chunkHash)
		if chunkData == nil {
			return false
		}
	}

	return true
}

func (n *node) allChunksKnown(fileInfo types.FileInfo) bool {
	for _, chunk := range fileInfo.Chunks {
		if chunk == nil {
			return false
		}
	}
	return true
}

// HW3

func (n *node) handlePaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	prepareMessage, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	if n.acceptor == nil {
		return fmt.Errorf("no acceptor")
	}

	promise := n.acceptor.HandlePrepareMessage(*prepareMessage)
	if promise != nil {
		promiseTransportMsg, _ := n.conf.MessageRegistry.MarshalMessage(promise)

		recipients := map[string]struct{}{
			prepareMessage.Source: {},
		}

		privateMsg := &types.PrivateMessage{
			Recipients: recipients,
			Msg:        &promiseTransportMsg,
		}

		privateTransportMsg, err := n.conf.MessageRegistry.MarshalMessage(privateMsg)
		if err != nil {
			return fmt.Errorf("error marshalling private message: %w", err)
		}

		err = n.Broadcast(privateTransportMsg)
		if err != nil {
			return fmt.Errorf("error broadcasting message")
		}

	}

	return nil
}

func (n *node) handlePaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	proposeMessage, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	if n.acceptor == nil {
		return fmt.Errorf("no acceptor")
	}

	accept := n.acceptor.HandleProposeMessage(*proposeMessage)
	if accept != nil {
		acceptTransportMsg, err := n.conf.MessageRegistry.MarshalMessage(accept)
		if err != nil {
			return fmt.Errorf("error marshalling accept message: %w", err)
		}

		err = n.Broadcast(acceptTransportMsg)
		if err != nil {
			return fmt.Errorf("error broadcasting message")
		}
	}
	return nil
}

func (n *node) handlePaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	promise, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}
	if promise.Step != n.proposer.GetStep() {
		return nil
	}

	n.proposer.CollectPromise(promise)
	return nil
}

func (n *node) handlePaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	accept, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}
	if accept.Step != n.proposer.GetStep() {
		return nil
	}

	thresholdReached := n.proposer.CollectAccept(accept)
	if thresholdReached {
		if !n.proposer.HasBroadcasted(int(accept.Step)) {

			filename := n.proposer.GetAccepts()[0].Value.Filename
			metahash := n.proposer.GetAccepts()[0].Value.Metahash

			if err := n.BroadcastTLCMessage(filename, metahash); err != nil {
				return fmt.Errorf("error broadcasting TLC message: %w", err)
			}
		}
	}
	return nil
}

func (n *node) handleTLCMessage(msg types.Message, pkt transport.Packet) error {
	tclMessage, ok := msg.(*types.TLCMessage)
	if !ok {
		return fmt.Errorf("unexpected message type")
	}

	if tclMessage.Step < n.proposer.GetStep() {
		return nil
	}

	if tclMessage.Step > n.proposer.GetStep() {
		n.futureMessages[tclMessage.Step] = append(n.futureMessages[tclMessage.Step], tclMessage)
		return nil
	}

	thresholdReached := n.proposer.CollectTLCMessage(tclMessage)
	if thresholdReached {

		n.blockPreProcessing(*tclMessage)

		err := n.blockBroadcast(*tclMessage)
		if err != nil {
			return fmt.Errorf("failed to broadcast TLC message")
		}

		n.proposer.IncrementStep()
		n.proposer.IncrementID()
		n.acceptor.IncrementStep()
		n.proposer.ResetPromises()
		n.proposer.ResetAccepts()
		n.acceptor.Reset()

		for {
			nextMessages, exists := n.futureMessages[n.proposer.GetStep()]
			if !exists {
				break
			}
			for _, nextMsg := range nextMessages {
				thresholdReached = n.proposer.CollectTLCMessage(nextMsg)
				if thresholdReached {
					n.blockPreProcessing(*nextMsg)

					n.proposer.IncrementStep()
					n.proposer.IncrementID()
					n.acceptor.IncrementStep()
					n.proposer.ResetPromises()
					n.proposer.ResetAccepts()
					n.acceptor.Reset()

				}
			}
		}
	}
	return nil
}

func (n *node) blockPreProcessing(tclMessage types.TLCMessage) {
	blockchainStore := n.conf.Storage.GetBlockchainStore()
	existingBlockData := blockchainStore.Get(hex.EncodeToString(tclMessage.Block.Hash))
	if existingBlockData == nil {
		n.processBlock(tclMessage.Block)
	}
}

func (n *node) blockBroadcast(tclMessage types.TLCMessage) error {
	if !n.proposer.HasBroadcasted(int(tclMessage.Step)) {
		tlcMsgTransport, _ := n.conf.MessageRegistry.MarshalMessage(tclMessage)
		n.proposer.MarkBroadcasted(int(tclMessage.Step))

		err := n.Broadcast(tlcMsgTransport)
		if err != nil {
			return fmt.Errorf("failed to broadcast TLC message")
		}
	}
	return nil
}

func (n *node) processBlock(block types.BlockchainBlock) {
	blockchainStore := n.conf.Storage.GetBlockchainStore()

	existingBlockData := blockchainStore.Get(hex.EncodeToString(block.Hash))
	if existingBlockData != nil {
		return
	}

	blockData, _ := block.Marshal()
	blockchainStore.Set(hex.EncodeToString(block.Hash), blockData)
	blockchainStore.Set(storage.LastBlockKey, block.Hash)
	namingStore := n.conf.Storage.GetNamingStore()
	namingStore.Set(block.Value.Filename, []byte(block.Value.Metahash))
}
