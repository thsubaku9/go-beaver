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

func Map[K any, R any](values []K, transform func(K) R) []R {
	res := make([]R, 0)
	for _, v := range values {
		res = append(res, transform(v))
	}

	return res
}

func ErrMap[K any](value K, transforms [](func(K) error)) error {
	for _, transform := range transforms {
		err := transform(value)
		if err != nil {
			return err
		}
	}
	return nil
}
