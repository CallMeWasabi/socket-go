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

func (s *HashSet[T]) Data() map[T]bool {
	return s.u
}

func (s *HashSet[T]) Insert(ele T) {
	if _, ok := s.u[ele]; ok {
		return
	}

	s.u[ele] = true
}

func (s *HashSet[T]) Remove(ele T) {
	if _, ok := s.u[ele]; !ok {
		return
	}

	delete(s.u, ele)
}

func (s *HashSet[T]) Contain(ele T) bool {
	_, ok := s.u[ele]
	return ok
}
