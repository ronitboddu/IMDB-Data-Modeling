package service

var exists = struct{}{}

type set struct {
	M map[int]struct{}
}

func NewSet() *set {
	s := &set{}
	s.M = make(map[int]struct{})
	return s
}

func (s *set) Add(value int) {
	s.M[value] = exists
}

func (s *set) Remove(value int) {
	delete(s.M, value)
}

func (s *set) Contains(value int) bool {
	_, c := s.M[value]
	return c
}
