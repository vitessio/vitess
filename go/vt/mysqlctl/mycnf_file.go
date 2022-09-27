package mysqlctl

func NewMycnfFromDefaultFile(tabletUID uint32) (*Mycnf, error) {
	return NewMycnfFromFile(tabletUID, MycnfFile(tabletUID))
}

func NewMycnfFromFile(tabletUID uint32, path string) (*Mycnf, error) {
	cnf := NewMycnf(tabletUID, 0)
	cnf.Path = path
	cnf, err := ReadMycnf(cnf)
	if err != nil {
		return nil, err
	}
	return cnf, nil
}
