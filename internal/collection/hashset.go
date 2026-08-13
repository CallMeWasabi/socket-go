package collection

// simple hash set implement
type HashSet[T comparable] struct {
	u map[T]bool
<<<<<<< HEAD
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

func (hs *HashSet[T]) Contain(ele T) bool {
	_, ok := hs.u[ele]
=======
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
>>>>>>> 558ba0f7335a6e2aef0b7a94b2f2033ec142da3d
	return ok
}

func (hs *HashSet[T]) Data() map[T]bool {
	return hs.u
}
