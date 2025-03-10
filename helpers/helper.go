package helpers

func Assert(cond bool) {
	if cond {
		return
	}

	panic("Assertion failed")
}

func AssertFn(condFn func() bool) {
	Assert(condFn())
}
