package mathutil

import "fmt"

type UnknownElementError struct {
	element string
}

func (e *UnknownElementError) Error() string {
	return fmt.Sprintf("unknown element %s", e.element)
}

type UnknownClassError struct {
	class int
}

func (e *UnknownClassError) Error() string {
	return fmt.Sprintf("unknown class %d", e.class)
}

// EquivalenceRelation implements a mathematical equivalence relation.
// Elements in this set are named, ie identified by strings.
// Elements are potentially grouped together in an equivalence relation. Each element belongs in exactly one
// relation, and each relation has 1 or more elements.
// If a,b are in the same relation, and if b,c are in the same relation, it follows that a,c are in the same relation.
// therefore two different entity relations cannot have any shared elements.
// Functions of this struct are not thread safe.
type EquivalenceRelation struct {
	elementClassMap  map[string]int
	classElementsMap map[int]([]string)
	classTagsMap     map[int](map[string]bool)

	classCounter int
}

func NewEquivalenceRelation() *EquivalenceRelation {
	return &EquivalenceRelation{
		elementClassMap:  make(map[string]int),
		classElementsMap: make(map[int][]string),
		classTagsMap:     make(map[int]map[string]bool),
	}
}

// Add adds a single element to the set. The element is associated with its own unique class
func (r *EquivalenceRelation) Add(element string) {
	if _, ok := r.elementClassMap[element]; ok {
		// element already exists
		return
	}
	r.elementClassMap[element] = r.classCounter
	r.classElementsMap[r.classCounter] = []string{element}
	r.classTagsMap[r.classCounter] = make(map[string]bool)
	r.classCounter++
}

// AddAll adds multiple elements to the set. Each element is associated with its own unique class
func (r *EquivalenceRelation) AddAll(elements []string) {
	for _, element := range elements {
		r.Add(element)
	}
}

// ElementClass returns the class id for the given element, or errors if the element is unknown
func (r *EquivalenceRelation) ElementClass(element string) (int, error) {
	class, ok := r.elementClassMap[element]
	if !ok {
		return 0, &UnknownElementError{element: element}
	}
	return class, nil
}

// Declare two elements to be associated in the same class. If they're already in the same class, nothing is done.
// Otherwise, this merges their two classes into one.
func (r *EquivalenceRelation) Relate(element1, element2 string) (int, error) {
	class1, err := r.ElementClass(element1)
	if err != nil {
		return class1, err
	}
	class2, err := r.ElementClass(element2)
	if err != nil {
		return class1, err
	}
	if class1 == class2 {
		// already associated
		return class1, nil
	}
	// We deterministically merge into the class with the lower Id
	if class1 > class2 {
		class1, class2 = class2, class1
	}
	r.classElementsMap[class1] = append(r.classElementsMap[class1], r.classElementsMap[class2]...)
	for _, element := range r.classElementsMap[class2] {
		r.elementClassMap[element] = class1
	}
	delete(r.classElementsMap, class2)

	// Also copy over all tags
	for tag := range r.classTagsMap[class2] {
		r.classTagsMap[class1][tag] = true
	}
	delete(r.classTagsMap, class2)

	return class1, nil
}

// Related returns true when both elements are in the same equivalence class. An error is returned if
// either element is unknown
func (r *EquivalenceRelation) Related(element1, element2 string) (bool, error) {
	class1, err := r.ElementClass(element1)
	if err != nil {
		return false, err
	}
	class2, err := r.ElementClass(element2)
	if err != nil {
		return false, err
	}
	return class1 == class2, nil
}

func (r *EquivalenceRelation) TagClass(class int, tag string) error {
	m, ok := r.classTagsMap[class]
	if !ok {
		return &UnknownClassError{class: class}
	}
	m[tag] = true
	return nil
}

func (r *EquivalenceRelation) TagElement(element string, tag string) error {
	class, err := r.ElementClass(element)
	if err != nil {
		return err
	}
	return r.TagClass(class, tag)
}

func (r *EquivalenceRelation) Map() map[int]([]string) {
	return r.classElementsMap
}

func (r *EquivalenceRelation) Tags(class int) (map[string]bool, error) {
	m, ok := r.classTagsMap[class]
	if !ok {
		return nil, &UnknownClassError{class: class}
	}
	return m, nil
}

func (r *EquivalenceRelation) ClassTagged(class int, tag string) (bool, error) {
	m, ok := r.classTagsMap[class]
	if !ok {
		return false, &UnknownClassError{class: class}
	}
	return m[tag], nil
}

func (r *EquivalenceRelation) ElementTagged(element string, tag string) (bool, error) {
	class, err := r.ElementClass(element)
	if err != nil {
		return false, err
	}
	return r.ClassTagged(class, tag)
}
