package helpers_test

import (
	"beaver/helpers"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertNoPanic(t *testing.T) {
	assert.NotPanics(t, func() { helpers.Assert(true) })
}

func TestAssertPanic(t *testing.T) {
	assert.Panics(t, func() { helpers.Assert(false) })
}
