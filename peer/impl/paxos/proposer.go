package paxos

import (
	"sync"
	"time"

	"go.dedis.ch/cs438/types"
)

type Proposer struct {
	Step         uint
	CurrentID    uint
	TotalPeers   uint
	RetryTimeout time.Duration
	Threshold    func(uint) int

	PromiseMutex             sync.Mutex
	acceptMutex              sync.Mutex
	TCLMutex                 sync.Mutex
	stepMutex                sync.Mutex
	idMutex                  sync.Mutex
	broadcastedMessagesMutex sync.Mutex

	Promises []*types.PaxosPromiseMessage
	Accepts  []*types.PaxosAcceptMessage

	PromiseCh chan struct{}
	AcceptCh  chan struct{}

	TLCMessages  []*types.TLCMessage
	TLCMessageCh chan struct{}

	broadcastedMessages map[int]bool
}

func NewProposer(step, initialID, totalPeers uint, retryTimeout time.Duration, threshold func(uint) int) *Proposer {
	return &Proposer{
		Step:         step,
		CurrentID:    initialID,
		TotalPeers:   totalPeers,
		RetryTimeout: retryTimeout,
		Threshold:    threshold,

		Promises: make([]*types.PaxosPromiseMessage, 0),
		Accepts:  make([]*types.PaxosAcceptMessage, 0),

		PromiseCh: make(chan struct{}, 1),
		AcceptCh:  make(chan struct{}, 1),

		broadcastedMessages: make(map[int]bool),
	}
}

func (p *Proposer) GetStep() uint {
	p.stepMutex.Lock()
	defer p.stepMutex.Unlock()
	return p.Step
}

func (p *Proposer) IncrementStep() {
	p.stepMutex.Lock()
	defer p.stepMutex.Unlock()
	p.Step++
}

func (p *Proposer) GetCurrentID() uint {
	p.idMutex.Lock()
	defer p.idMutex.Unlock()
	return p.CurrentID
}

func (p *Proposer) IncrementID() {
	p.idMutex.Lock()
	defer p.idMutex.Unlock()
	p.CurrentID += p.TotalPeers
}

func (p *Proposer) GetPromises() []*types.PaxosPromiseMessage {
	p.PromiseMutex.Lock()
	defer p.PromiseMutex.Unlock()

	return p.Promises
}

func (p *Proposer) GetAccepts() []*types.PaxosAcceptMessage {
	p.acceptMutex.Lock()
	defer p.acceptMutex.Unlock()

	return p.Accepts
}

func (p *Proposer) ResetPromises() {
	p.PromiseMutex.Lock()
	defer p.PromiseMutex.Unlock()

	p.Promises = make([]*types.PaxosPromiseMessage, 0)
}

func (p *Proposer) ResetAccepts() {
	p.acceptMutex.Lock()
	defer p.acceptMutex.Unlock()

	p.Accepts = make([]*types.PaxosAcceptMessage, 0)
}

func (p *Proposer) CollectPromise(promise *types.PaxosPromiseMessage) bool {
	p.PromiseMutex.Lock()
	defer p.PromiseMutex.Unlock()

	p.Promises = append(p.Promises, promise)
	if len(p.Promises) >= p.Threshold(p.TotalPeers) {
		select {
		case p.PromiseCh <- struct{}{}:
		default:
		}
		return true
	}
	return false
}

func (p *Proposer) CollectAccept(accept *types.PaxosAcceptMessage) bool {
	p.acceptMutex.Lock()
	defer p.acceptMutex.Unlock()

	p.Accepts = append(p.Accepts, accept)
	if len(p.Accepts) >= p.Threshold(p.TotalPeers) {
		select {
		case p.AcceptCh <- struct{}{}:
		default:
		}
		return true
	}
	return false
}

func (p *Proposer) CollectTLCMessage(tlcMessage *types.TLCMessage) bool {
	p.TCLMutex.Lock()
	defer p.TCLMutex.Unlock()

	p.TLCMessages = append(p.TLCMessages, tlcMessage)

	if len(p.TLCMessages) >= p.Threshold(p.TotalPeers) {
		select {
		case p.TLCMessageCh <- struct{}{}:
		default:
		}
		return true
	}
	return false
}

func (p *Proposer) HasBroadcasted(step int) bool {
	p.broadcastedMessagesMutex.Lock()
	defer p.broadcastedMessagesMutex.Unlock()
	return p.broadcastedMessages[step]
}

func (p *Proposer) MarkBroadcasted(step int) {
	p.broadcastedMessagesMutex.Lock()
	defer p.broadcastedMessagesMutex.Unlock()
	p.broadcastedMessages[step] = true
}
