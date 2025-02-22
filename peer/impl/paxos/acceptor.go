package paxos

import (
	"sync"

	"go.dedis.ch/cs438/types"
)

type Acceptor struct {
	Step             uint
	MaxID            uint
	ProposalAccepted bool
	AcceptedID       uint
	AcceptedValue    *types.PaxosValue

	stepMutex sync.Mutex

	acceptorMutex sync.Mutex
}

func NewAcceptor(initialStep uint) *Acceptor {
	return &Acceptor{
		Step:             initialStep,
		MaxID:            0,
		ProposalAccepted: false,
		AcceptedID:       0,
		AcceptedValue:    nil,
	}
}

func (a *Acceptor) HandlePrepareMessage(msg types.PaxosPrepareMessage) *types.PaxosPromiseMessage {
	if msg.Step != a.GetStep() {
		return nil
	}

	a.acceptorMutex.Lock()
	defer a.acceptorMutex.Unlock()

	if msg.ID <= a.MaxID {
		return nil
	}

	a.MaxID = msg.ID
	if a.ProposalAccepted {
		return &types.PaxosPromiseMessage{
			Step:          a.GetStep(),
			ID:            msg.ID,
			AcceptedID:    a.AcceptedID,
			AcceptedValue: a.AcceptedValue,
		}
	}

	return &types.PaxosPromiseMessage{
		Step: a.GetStep(),
		ID:   msg.ID,
	}
}

func (a *Acceptor) HandleProposeMessage(msg types.PaxosProposeMessage) *types.PaxosAcceptMessage {
	if msg.Step != a.GetStep() {
		return nil
	}

	a.acceptorMutex.Lock()
	defer a.acceptorMutex.Unlock()

	if msg.ID != a.MaxID {
		return nil
	}

	a.ProposalAccepted = true
	a.AcceptedID = msg.ID
	a.AcceptedValue = &msg.Value

	return &types.PaxosAcceptMessage{
		Step:  a.GetStep(),
		ID:    msg.ID,
		Value: msg.Value,
	}
}

func (a *Acceptor) Reset() {
	a.acceptorMutex.Lock()
	defer a.acceptorMutex.Unlock()

	a.MaxID = 0
	a.ProposalAccepted = false
	a.AcceptedID = 0
	a.AcceptedValue = nil
}

func (a *Acceptor) GetStep() uint {
	a.stepMutex.Lock()
	defer a.stepMutex.Unlock()
	return a.Step
}

func (a *Acceptor) IncrementStep() {
	a.stepMutex.Lock()
	defer a.stepMutex.Unlock()
	a.Step++
}
