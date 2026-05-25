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

type EnvironmentData struct {
	Addresses      string
	Max_Goroutines int
	Period         int
}

func NewEnvironmentData(a string, m int, p int) *EnvironmentData {
	return &EnvironmentData{
		Addresses:      a,
		Max_Goroutines: m,
		Period:         p,
	}
}
