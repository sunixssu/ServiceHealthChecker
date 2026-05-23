package storage

type Statuslist struct {
	Link        string `json:link`
	StatusCode  int    `json:statuscode`
	State       string `json:state`
	TimeChecked string `json:timeChecked`
}

func NewStatuslist() *Statuslist {
	return &Statuslist{Link: "", StatusCode: 0, State: "", TimeChecked: ""}
}

func (sl *Statuslist) AddStatus(l string, sc int, s string, t string) {
	sl.Link = l
	sl.StatusCode = sc
	sl.State = s
	sl.TimeChecked = t
}

func (sl *Statuslist) ChangeState(sc int, s string, t string) {
	sl.State = s
	sl.StatusCode = sc
	sl.TimeChecked = t
}
