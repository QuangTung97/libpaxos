package paxos_test

import (
	"fmt"
	"math/rand"
	"slices"
	"time"

	. "github.com/QuangTung97/libpaxos/paxos"
)

func getRandomActionKey[V any](
	randObj *rand.Rand, inputMap map[simulateActionKey]V,
) (simulateActionKey, bool) {
	if len(inputMap) == 0 {
		return simulateActionKey{}, false
	}

	keys := make([]simulateActionKey, 0, len(inputMap))
	for k := range inputMap {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, compareActionKey)

	index := randObj.Intn(len(keys))
	return keys[index], true
}

func newRandomObject() *rand.Rand {
	seed := time.Now().UnixNano()
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
) {
	totalWeight := 0
	for _, action := range possibleActions {
		totalWeight += action.weight
	}

	if totalWeight <= 0 {
		return
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
			return
		}
	}
}

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
				st.cmdChan <- fmt.Sprintf("new command: %d", nextCmd)
			}
		},
	)
}
