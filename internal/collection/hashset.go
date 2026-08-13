package collection

// simple hash set implement
type HashSet[T comparable] struct {
	u map[T]bool
}

func NewHashSet[T comparable]() HashSet[T] {
	return HashSet[T]{
		u: make(map[T]bool),
	}
}

func (hs *HashSet[T]) Insert(ele T) {
	if _, ok := hs.u[ele]; ok {
		return
	}

	hs.u[ele] = true
}

func (hs *HashSet[T]) Remove(ele T) {
	if _, ok := hs.u[ele]; !ok {
		return
	}

	delete(hs.u, ele)
}

func (s *HashSet[T]) Contain(ele T) bool {
	_, ok := s.u[ele]
	return ok
}

func (hs *HashSet[T]) Data() map[T]bool {
	return hs.u
}
