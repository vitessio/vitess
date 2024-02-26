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

type TextualAnnotationType int

const (
	UnchangedTextualAnnotationType TextualAnnotationType = iota
	AddedTextualAnnotationType
	RemovedTextualAnnotationType
)

type AnnotatedText struct {
	text string
	typ  TextualAnnotationType
}

type TextualAnnotations struct {
	texts      []*AnnotatedText
	hasChanges bool
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
		a.hasChanges = true
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
			if a.hasChanges {
				// If there is absolutely no change, we don't add a space anywhere
				annotatedText.text = " " + annotatedText.text
			}
		}
		textLines = append(textLines, annotatedText.text)
	}
	return strings.Join(textLines, "\n")
}

func annotatedStatement(stmt string, annotationType TextualAnnotationType, annotations *TextualAnnotations) *TextualAnnotations {
	stmtLines := strings.Split(stmt, "\n")
	result := NewTextualAnnotations()
	annotationLines := map[string]bool{}
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
			possibleMutations := map[string]bool{
				annotationLine:              true,
				") " + annotationLine:       true,
				") " + annotationLine + ",": true,
				"(" + annotationLine:        true,
				"(" + annotationLine + ",":  true,
				annotationLine + ",":        true,
				annotationLine + ")":        true,
			}
			if possibleMutations[trimmedLine] {
				// Annotate this line!
				result.mark(stmtLines[i], annotationType)
				lineAnnotated = true
				delete(annotationLines, annotationLine) // no need to iterate it in the future
			}
		}
		if !lineAnnotated {
			result.MarkUnchanged(stmtLines[i])
		}
	}
	return result
}

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
