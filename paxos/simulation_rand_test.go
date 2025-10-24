package paxos_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync/atomic"
	"time"

	. "github.com/QuangTung97/libpaxos/paxos"
)

func getRandomActionKey[V any](
	randObj *rand.Rand, inputMap map[simulateActionKey]V,
) (simulateActionKey, bool) {
	if len(inputMap) == 0 {
		return simulateActionKey{}, false
	}

	// get all keys
	keys := make([]simulateActionKey, 0, len(inputMap))
	for k := range inputMap {
		keys = append(keys, k)
	}

	// sort by a deterministic order
	slices.SortFunc(keys, compareActionKey)

	// get a random key
	index := randObj.Intn(len(keys))
	return keys[index], true
}

func newRandomObject(initSeed int64) *rand.Rand {
	seed := time.Now().UnixNano()
	if initSeed > 0 {
		seed = initSeed
	}
	fmt.Println("SEED:", seed)
	return rand.New(rand.NewSource(seed))
}

type actionWithWeightInfo struct {
	fn     func()
	weight int
}

func randomActionWeight(weight int, actionFn func()) actionWithWeightInfo {
	return actionWithWeightInfo{
		fn:     actionFn,
		weight: weight,
	}
}

func runRandomAction(
	randObj *rand.Rand,
	possibleActions ...actionWithWeightInfo,
) bool {
	totalWeight := 0
	for _, action := range possibleActions {
		totalWeight += action.weight
	}

	if totalWeight <= 0 {
		return false
	}

	chosenWeight := randObj.Intn(totalWeight)

	checkWeight := 0
	for _, action := range possibleActions {
		if action.weight <= 0 {
			continue
		}

		checkWeight += action.weight
		if checkWeight > chosenWeight {
			action.fn()
			return true
		}
	}

	return false
}

// randomExecAction run a random action
func randomExecAction(
	randObj *rand.Rand,
	inputMap map[simulateActionKey]chan struct{},
) actionWithWeightInfo {
	return randomActionWeight(len(inputMap), func() {
		key, ok := getRandomActionKey(randObj, inputMap)
		if ok {
			waitCh := inputMap[key]
			delete(inputMap, key)
			close(waitCh)
		}
	})
}

// randomExecActionIgnoreNode run a random action except all actions of the ignored node
func randomExecActionIgnoreNode(
	randObj *rand.Rand,
	inputMap map[simulateActionKey]chan struct{},
	ignoredNode NodeID,
) actionWithWeightInfo {
	checkMap := maps.Clone(inputMap)
	for key := range checkMap {
		if key.toNode == ignoredNode {
			delete(checkMap, key)
		}
	}

	return randomActionWeight(len(checkMap), func() {
		key, ok := getRandomActionKey(randObj, checkMap)
		if ok {
			waitCh := inputMap[key]
			delete(inputMap, key)
			close(waitCh)
		}
	})
}

func randomNetworkDisconnect(
	randObj *rand.Rand,
	activeConn map[simulateActionKey]SimulationConn,
	numTimes *int,
	maxNumTimes int,
) actionWithWeightInfo {
	weight := len(activeConn)
	if *numTimes >= maxNumTimes {
		weight = 0
	}

	return randomActionWeight(weight, func() {
		*numTimes++
		key, ok := getRandomActionKey(randObj, activeConn)
		if ok {
			conn := activeConn[key]
			delete(activeConn, key)
			conn.CloseConn()
		}
	})
}

func randomSendCmdToLeader(
	nodeMap map[NodeID]*simulateNodeState,
	nextCmd *int,
	maxCmdNum int,
) actionWithWeightInfo {
	cmdWeight := 1
	if *nextCmd >= maxCmdNum {
		cmdWeight = 0
	}

	return randomActionWeight(
		cmdWeight,
		func() {
			for _, st := range nodeMap {
				core := st.core
				if core.GetState() != StateLeader {
					continue
				}
				*nextCmd++
				st.cmdChan <- fmt.Sprintf("new command: %d", *nextCmd)
			}
		},
	)
}

func randomChangeLeader(
	randObj *rand.Rand,
	nodeMap map[NodeID]*simulateNodeState,
	currentNumChange *int,
	maxNumChange int,
) actionWithWeightInfo {
	cmdWeight := 1
	if *currentNumChange >= maxNumChange {
		cmdWeight = 0
	}

	nodes := []NodeID{
		nodeID1, nodeID2, nodeID3,
		nodeID4, nodeID5, nodeID6,
	}
	randObj.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	// random from 1 => 3 nodes
	numNodes := randObj.Intn(3) + 1
	randomNodes := nodes[:numNodes]

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel to avoid blocking

	return randomActionWeight(
		cmdWeight,
		func() {
			for _, st := range nodeMap {
				core := st.core
				if core.GetState() != StateLeader {
					continue
				}

				err := core.ChangeMembership(ctx, st.persistent.GetLastTerm(), randomNodes)
				if err != nil {
					continue
				}
				*currentNumChange++
			}
		},
	)
}

func randomTimeout(
	now *atomic.Int64,
	randObj *rand.Rand,
	nodeMap map[NodeID]*simulateNodeState,
	currentNumChange *int,
	maxNumChange int,
) actionWithWeightInfo {
	cmdWeight := 1
	if *currentNumChange >= maxNumChange {
		cmdWeight = 0
	}

	// get all ids
	idList := make([]NodeID, 0, len(nodeMap))
	for id := range nodeMap {
		idList = append(idList, id)
	}
	slices.SortFunc(idList, CompareNodeID)

	// get random node id
	index := randObj.Intn(len(idList))
	nodeID := idList[index]

	return randomActionWeight(
		cmdWeight,
		func() {
			*currentNumChange++
			now.Add(11_000)
			nodeMap[nodeID].core.CheckTimeout()
		},
	)
}
