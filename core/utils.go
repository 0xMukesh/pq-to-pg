package core

func SliceToChan[T any](s []T) <-chan T {
	ch := make(chan T)

	go func() {
		for _, item := range s {
			ch <- item
		}
		close(ch)
	}()

	return ch
}
