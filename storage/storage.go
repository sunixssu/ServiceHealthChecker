package storage

type Storage struct {
	Requests []Statuslist `json:requests`
}

func NewStorage() *Storage {
	return &Storage{}
}

func (s *Storage) AddRequest(req Statuslist) {
	s.Requests = append(s.Requests, req)
}
