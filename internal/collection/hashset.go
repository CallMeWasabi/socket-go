package collection

// simple hash set implement
type HashSet[T comparable] struct {
	U map[T]bool
}

func (s *HashSet[T]) Insert(ele T) {
	if _, ok := s.U[ele]; ok {
		return
	}

	s.U[ele] = true
}

func (s *HashSet[T]) Remove(ele T) {
	if _, ok := s.U[ele]; !ok {
		return
	}

	delete(s.U, ele)
}

func (s *HashSet[T]) Contain(ele T) bool {
	_, ok := s.U[ele]
	return ok
}
