/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schemadiff

import (
	"strings"
)

// TextualAnnotationType is an enum for the type of annotation that can be applied to a line of text.
type TextualAnnotationType int

const (
	UnchangedTextualAnnotationType TextualAnnotationType = iota
	AddedTextualAnnotationType
	RemovedTextualAnnotationType
)

// AnnotatedText is a some text and its annotation type. The text is usually single-line, but it
// can be multi-line, as in the case of partition specs.
type AnnotatedText struct {
	text string
	typ  TextualAnnotationType
}

// TextualAnnotations is a sequence of annotated texts. It is the annotated representation of a statement.
type TextualAnnotations struct {
	texts         []*AnnotatedText
	hasAnyChanges bool
}

func NewTextualAnnotations() *TextualAnnotations {
	return &TextualAnnotations{}
}

func (a *TextualAnnotations) Len() int {
	return len(a.texts)
}

func (a *TextualAnnotations) mark(text string, typ TextualAnnotationType) {
	a.texts = append(a.texts, &AnnotatedText{text: text, typ: typ})
	if typ != UnchangedTextualAnnotationType {
		a.hasAnyChanges = true
	}
}

func (a *TextualAnnotations) MarkAdded(text string) {
	a.mark(text, AddedTextualAnnotationType)
}

func (a *TextualAnnotations) MarkRemoved(text string) {
	a.mark(text, RemovedTextualAnnotationType)
}

func (a *TextualAnnotations) MarkUnchanged(text string) {
	a.mark(text, UnchangedTextualAnnotationType)
}

// ByType returns the subset of annotations by given type.
func (a *TextualAnnotations) ByType(typ TextualAnnotationType) (r []*AnnotatedText) {
	for _, text := range a.texts {
		if text.typ == typ {
			r = append(r, text)
		}
	}
	return r
}

func (a *TextualAnnotations) Added() (r []*AnnotatedText) {
	return a.ByType(AddedTextualAnnotationType)
}

func (a *TextualAnnotations) Removed() (r []*AnnotatedText) {
	return a.ByType(RemovedTextualAnnotationType)
}

// Export beautifies the annotated text and returns it as a string.
func (a *TextualAnnotations) Export() string {
	textLines := make([]string, 0, len(a.texts))
	for _, annotatedText := range a.texts {
		switch annotatedText.typ {
		case AddedTextualAnnotationType:
			annotatedText.text = "+" + annotatedText.text
		case RemovedTextualAnnotationType:
			annotatedText.text = "-" + annotatedText.text
		default:
			// text unchanged
			if a.hasAnyChanges {
				// If there is absolutely no change, we don't add a space anywhere
				annotatedText.text = " " + annotatedText.text
			}
		}
		textLines = append(textLines, annotatedText.text)
	}
	return strings.Join(textLines, "\n")
}

// annotatedStatement returns a new TextualAnnotations object that annotates the given statement with the given annotations.
// The given annotations were created by the diffing algorithm, and represent the CanonicalString of some node.
// However, the given statement is just some text, and we need to find the annotations (some of which may be multi-line)
// inside our text, and return a per-line annotation.
func annotatedStatement(stmt string, annotationType TextualAnnotationType, annotations *TextualAnnotations) *TextualAnnotations {
	stmtLines := strings.Split(stmt, "\n")
	result := NewTextualAnnotations()
	annotationLines := map[string]bool{} // single-line breakdown of all annotations
	for _, annotation := range annotations.ByType(annotationType) {
		// An annotated text could be multiline. Partition specs are such.
		lines := strings.Split(annotation.text, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				annotationLines[line] = true
			}
		}
	}
	annotationLinesMutations := map[string](map[string]bool){}
	// Mutations are expected ways to find an annotation inside a `CREATE TABLE` statement.
	for annotationLine := range annotationLines {
		possibleMutations := map[string]bool{
			annotationLine:              true,
			") " + annotationLine:       true, // e.g. ") ENGINE=InnoDB"
			") " + annotationLine + ",": true, // e.g. ") ENGINE=InnoDB,[\n	 ROW_FORMAT=COMPRESSED]"
			"(" + annotationLine + ")":  true, // e.g. "(PARTITION p0 VALUES LESS THAN (10))
			"(" + annotationLine + ",":  true, // e.g. "(PARTITION p0 VALUES LESS THAN (10),
			annotationLine + ",":        true, // e.g. "i int unsigned,"
			annotationLine + ")":        true, // e.g. "PARTITION p9 VALUES LESS THAN (90))"
		}
		annotationLinesMutations[annotationLine] = possibleMutations
	}
	for i := range stmtLines {
		lineAnnotated := false
		trimmedLine := strings.TrimSpace(stmtLines[i])
		if trimmedLine == "" {
			continue
		}
		for annotationLine := range annotationLines {
			if lineAnnotated {
				break
			}
			possibleMutations := annotationLinesMutations[annotationLine]
			if possibleMutations[trimmedLine] {
				// Annotate this line!
				result.mark(stmtLines[i], annotationType)
				lineAnnotated = true
				// No need to match this annotation again
				delete(annotationLines, annotationLine)
				delete(possibleMutations, annotationLine)
			}
		}
		if !lineAnnotated {
			result.MarkUnchanged(stmtLines[i])
		}
	}
	return result
}

// annotateAll blindly annotates all lines of the given statement with the given annotation type.
func annotateAll(stmt string, annotationType TextualAnnotationType) *TextualAnnotations {
	stmtLines := strings.Split(stmt, "\n")
	result := NewTextualAnnotations()
	for _, line := range stmtLines {
		result.mark(line, annotationType)
	}
	return result
}

// unifiedAnnotated takes two annotations of from, to statements and returns a unified annotation.
func unifiedAnnotated(from *TextualAnnotations, to *TextualAnnotations) *TextualAnnotations {
	unified := NewTextualAnnotations()
	fromIndex := 0
	toIndex := 0
	for fromIndex < from.Len() || toIndex < to.Len() {
		matchingLine := ""
		if fromIndex < from.Len() {
			fromLine := from.texts[fromIndex]
			if fromLine.typ == RemovedTextualAnnotationType {
				unified.MarkRemoved(fromLine.text)
				fromIndex++
				continue
			}
			matchingLine = fromLine.text
		}
		if toIndex < to.Len() {
			toLine := to.texts[toIndex]
			if toLine.typ == AddedTextualAnnotationType {
				unified.MarkAdded(toLine.text)
				toIndex++
				continue
			}
			if matchingLine == "" {
				matchingLine = toLine.text
			}
		}
		unified.MarkUnchanged(matchingLine)
		fromIndex++
		toIndex++
	}
	return unified
}

// annotatedDiff returns the annotated representations of the from and to entities, and their unified representation.
func annotatedDiff(diff EntityDiff, entityAnnotations *TextualAnnotations) (from *TextualAnnotations, to *TextualAnnotations, unified *TextualAnnotations) {
	fromEntity, toEntity := diff.Entities()
	// Handle the infamous golang interface is not-nil but underlying object is:
	if fromEntity != nil && fromEntity.Create() == nil {
		fromEntity = nil
	}
	if toEntity != nil && toEntity.Create() == nil {
		toEntity = nil
	}
	switch {
	case fromEntity == nil && toEntity == nil:
		// Will only get here if using mockup entities, as generated by EntityDiffByStatement.
		return nil, nil, nil
	case fromEntity == nil:
		// A new entity was created.
		from = NewTextualAnnotations()
		to = annotateAll(toEntity.Create().CanonicalStatementString(), AddedTextualAnnotationType)
	case toEntity == nil:
		// An entity was dropped.
		from = annotateAll(fromEntity.Create().CanonicalStatementString(), RemovedTextualAnnotationType)
		to = NewTextualAnnotations()
	case entityAnnotations == nil:
		// Entity was modified, and we have no prior info about entity annotations. Treat this is as a complete rewrite.
		from = annotateAll(fromEntity.Create().CanonicalStatementString(), RemovedTextualAnnotationType)
		to = annotateAll(toEntity.Create().CanonicalStatementString(), AddedTextualAnnotationType)
	default:
		// Entity was modified, and we have prior info about entity annotations.
		from = annotatedStatement(fromEntity.Create().CanonicalStatementString(), RemovedTextualAnnotationType, entityAnnotations)
		to = annotatedStatement(toEntity.Create().CanonicalStatementString(), AddedTextualAnnotationType, entityAnnotations)
	}
	return from, to, unifiedAnnotated(from, to)
}
