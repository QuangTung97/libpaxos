package key_runner

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

type objectValue struct {
	key string
	val int
}

func (o objectValue) getKey() string {
	return o.key
}

func clearContexts(list []startEntry[objectValue]) []startEntry[objectValue] {
	list = slices.Clone(list)
	for i := range list {
		list[i].ctx = nil
	}
	return list
}

type orderedAndCmp interface {
	comparable
	cmp.Ordered
}

func getKeys[K orderedAndCmp, V any](m map[K]V) []K {
	result := make([]K, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	slices.Sort(result)
	return result
}

func TestKeyRunner__Upsert_With_Remove(t *testing.T) {
	r := New(objectValue.getKey, nil)

	startList, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
		{key: "key03", val: 13},
	})
	assert.Equal(t, true, updated)

	// check start list
	assert.Equal(t, []startEntry[objectValue]{
		{val: objectValue{key: "key01", val: 11}},
		{val: objectValue{key: "key02", val: 12}},
		{val: objectValue{key: "key03", val: 13}},
	}, clearContexts(startList))

	// no context is canceled
	assert.Equal(t, nil, startList[0].ctx.Err())
	assert.Equal(t, nil, startList[2].ctx.Err())

	// remove key 01, key 02
	startList2, updated := r.upsertInternal([]objectValue{
		{key: "key03", val: 13},
	})
	assert.Equal(t, 0, len(startList2))
	assert.Equal(t, true, updated)

	// check context canceled
	assert.Equal(t, context.Canceled, startList[0].ctx.Err())
	assert.Equal(t, context.Canceled, startList[1].ctx.Err())
	assert.Equal(t, nil, startList[2].ctx.Err())

	// check active key
	assert.Equal(t, map[string]struct{}{
		"key03": {},
	}, r.activeKeys)

	// check running thread
	assert.Equal(t, []string{"key01", "key02", "key03"}, getKeys(r.running))
}

func TestKeyRunner__Upsert__Then_Remove__Then_Finish(t *testing.T) {
	r := New(objectValue.getKey, nil)

	startList, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
	})
	assert.Equal(t, true, updated)

	// check start list
	assert.Equal(t, []startEntry[objectValue]{
		{val: objectValue{key: "key01", val: 11}},
		{val: objectValue{key: "key02", val: 12}},
	}, clearContexts(startList))

	// no context is canceled
	assert.Equal(t, nil, startList[0].ctx.Err())
	assert.Equal(t, nil, startList[1].ctx.Err())

	// remove key01
	startList2, updated := r.upsertInternal([]objectValue{
		{key: "key02", val: 12},
	})
	assert.Equal(t, 0, len(startList2))
	assert.Equal(t, true, updated)

	// check context
	assert.Equal(t, context.Canceled, startList[0].ctx.Err())
	assert.Equal(t, nil, startList[1].ctx.Err())

	// finish key01
	startEntry1, ok := r.finishInternal("key01")
	assert.Equal(t, false, ok)
	assert.Equal(t, startEntry[objectValue]{}, startEntry1)

	// check active key
	assert.Equal(t, map[string]struct{}{
		"key02": {},
	}, r.activeKeys)

	// check running thread
	assert.Equal(t, []string{"key02"}, getKeys(r.running))
}

func TestKeyRunner__Update_Value__Then_Finish(t *testing.T) {
	r := New(objectValue.getKey, nil)

	startList, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
	})
	assert.Equal(t, true, updated)

	// check start list
	assert.Equal(t, []startEntry[objectValue]{
		{val: objectValue{key: "key01", val: 11}},
		{val: objectValue{key: "key02", val: 12}},
	}, clearContexts(startList))

	// do update
	startList2, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 21},
		{key: "key02", val: 22},
	})
	assert.Equal(t, 0, len(startList2))
	assert.Equal(t, true, updated)

	// check context canceled
	assert.Equal(t, context.Canceled, startList[0].ctx.Err())
	assert.Equal(t, context.Canceled, startList[1].ctx.Err())

	// finish key01
	startEntry3, ok := r.finishInternal("key01")
	assert.Equal(t, true, ok)

	// check start entry
	assert.Equal(t, nil, startEntry3.ctx.Err())
	startEntry3.ctx = nil
	assert.Equal(t, startEntry[objectValue]{
		val: objectValue{key: "key01", val: 21},
	}, startEntry3)

	// check active key
	assert.Equal(t, map[string]struct{}{
		"key01": {},
		"key02": {},
	}, r.activeKeys)

	// check running thread
	assert.Equal(t, []string{"key01", "key02"}, getKeys(r.running))
}

func TestKeyRunner__Remove_Then_Add_Again__Before_Finish(t *testing.T) {
	r := New(objectValue.getKey, nil)

	startList, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
	})
	assert.Equal(t, true, updated)

	// check start list
	assert.Equal(t, []startEntry[objectValue]{
		{val: objectValue{key: "key01", val: 11}},
		{val: objectValue{key: "key02", val: 12}},
	}, clearContexts(startList))

	// remove key01
	startList2, updated := r.upsertInternal([]objectValue{
		{key: "key02", val: 12},
	})
	assert.Equal(t, 0, len(startList2))
	assert.Equal(t, true, updated)

	// add again & updated
	startList3, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 21},
		{key: "key02", val: 12},
	})
	assert.Equal(t, 0, len(startList3))
	assert.Equal(t, true, updated)

	// check context canceled
	assert.Equal(t, context.Canceled, startList[0].ctx.Err())
	assert.Equal(t, nil, startList[1].ctx.Err())

	// check active key
	assert.Equal(t, map[string]struct{}{
		"key01": {},
		"key02": {},
	}, r.activeKeys)

	// check running thread
	assert.Equal(t, []string{"key01", "key02"}, getKeys(r.running))

	// finish key01
	entry1, ok := r.finishInternal("key01")
	assert.Equal(t, true, ok)
	assert.Equal(t, nil, entry1.ctx.Err())
	assert.Equal(t, objectValue{key: "key01", val: 21}, entry1.val)
}

func TestKeyRunner__Upsert_Same_Not_Updated(t *testing.T) {
	r := New(objectValue.getKey, nil)

	startList, updated := r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
	})
	assert.Equal(t, true, updated)

	// check start list
	assert.Equal(t, []startEntry[objectValue]{
		{val: objectValue{key: "key01", val: 11}},
		{val: objectValue{key: "key02", val: 12}},
	}, clearContexts(startList))

	// update the same
	startList, updated = r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
	})
	assert.Equal(t, false, updated)
	assert.Equal(t, 0, len(startList))

	// remove key01
	startList, updated = r.upsertInternal([]objectValue{
		{key: "key02", val: 12},
	})
	assert.Equal(t, true, updated)
	assert.Equal(t, 0, len(startList))

	// keep the same
	startList, updated = r.upsertInternal([]objectValue{
		{key: "key02", val: 12},
	})
	assert.Equal(t, false, updated)
	assert.Equal(t, 0, len(startList))

	// add back key 01
	startList, updated = r.upsertInternal([]objectValue{
		{key: "key01", val: 11},
		{key: "key02", val: 12},
	})
	assert.Equal(t, true, updated)
	assert.Equal(t, 0, len(startList))
}

func TestKeyRunner_Public(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runningMut sync.Mutex
		runningSet := map[string]objectValue{}

		r := New(
			objectValue.getKey,
			func(ctx context.Context, val objectValue) {
				runningMut.Lock()
				runningSet[val.key] = val
				runningMut.Unlock()

				<-ctx.Done()

				runningMut.Lock()
				delete(runningSet, val.key)
				runningMut.Unlock()
			},
		)

		// insert
		r.Upsert([]objectValue{
			{key: "key01", val: 11},
			{key: "key02", val: 12},
		})

		synctest.Wait()

		// check running set
		runningMut.Lock()
		assert.Equal(t, map[string]objectValue{
			"key01": {key: "key01", val: 11},
			"key02": {key: "key02", val: 12},
		}, runningSet)
		runningMut.Unlock()

		// delete key01
		r.Upsert([]objectValue{
			{key: "key02", val: 12},
		})

		synctest.Wait()

		// check running set
		runningMut.Lock()
		assert.Equal(t, map[string]objectValue{
			"key02": {key: "key02", val: 12},
		}, runningSet)
		runningMut.Unlock()

		// update key 02 & insert key 03
		r.Upsert([]objectValue{
			{key: "key02", val: 22},
			{key: "key03", val: 13},
		})

		synctest.Wait()

		// check running set
		runningMut.Lock()
		assert.Equal(t, map[string]objectValue{
			"key02": {key: "key02", val: 22},
			"key03": {key: "key03", val: 13},
		}, runningSet)
		runningMut.Unlock()

		r.Shutdown()
		synctest.Wait()

		// check running set after shutdown
		runningMut.Lock()
		assert.Equal(t, map[string]objectValue{}, runningSet)
		runningMut.Unlock()
	})
}
