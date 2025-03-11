package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertNoPanic(t *testing.T) {
	assert.NotPanics(t, func() { Assert(true) })
}

func TestAssertPanic(t *testing.T) {
	assert.Panics(t, func() { Assert(false) })
}

func TestAssertFnNoPanic(t *testing.T) {
	assert.NotPanics(t, func() { AssertFn(func() bool { return true }) })
}

func TestAssertFnPanic(t *testing.T) {
	assert.Panics(t, func() { AssertFn(func() bool { return false }) })
}
