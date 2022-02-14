package uca

type Reorder struct {
	FromMin, FromMax uint16
	ToMin, ToMax     uint16
}

type parametricT struct {
	upperCaseFirst bool
	reorder        []Reorder
	reorderMin     uint16
	reorderMax     uint16
}

func (p *parametricT) adjust(level int, weight uint16) uint16 {
	if p == nil {
		return weight
	}
	switch level {
	case 0:
		if weight >= p.reorderMin && weight <= p.reorderMax {
			for _, reorder := range p.reorder {
				if weight >= reorder.FromMin && weight <= reorder.FromMax {
					return weight - reorder.FromMin + reorder.ToMin
				}
			}
		}
	case 2:
		// see: https://unicode.org/reports/tr35/tr35-collation.html#Case_Untailored
		if p.upperCaseFirst && weight < ' ' {
			switch weight {
			case 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0E, 0x11, 0x12, 0x1D:
				return weight | 0x100
			default:
				return weight | 0x300
			}
		}
	}
	return weight
}

func newParametricTailoring(reorder []Reorder, upperCaseFirst bool) *parametricT {
	if len(reorder) == 0 && !upperCaseFirst {
		return nil
	}

	t := &parametricT{
		upperCaseFirst: upperCaseFirst,
		reorder:        reorder,
		reorderMin:     ^uint16(0),
		reorderMax:     0,
	}

	for _, r := range reorder {
		if r.FromMin < t.reorderMin {
			t.reorderMin = r.FromMin
		}
		if r.FromMax > t.reorderMax {
			t.reorderMax = r.FromMax
		}
	}

	return t
}
