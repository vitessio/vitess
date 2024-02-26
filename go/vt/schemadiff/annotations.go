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

type TextualAnnotatedLine struct {
	line string
	typ  TextualAnnotationType
}

type TextualAnnotations struct {
	lines      []*TextualAnnotatedLine
	hasChanges bool
}

func NewTextualAnnotations() *TextualAnnotations {
	return &TextualAnnotations{}
}

func (a *TextualAnnotations) Len() int {
	return len(a.lines)
}

func (a *TextualAnnotations) mark(line string, typ TextualAnnotationType) {
	a.lines = append(a.lines, &TextualAnnotatedLine{line: line, typ: typ})
	if typ != UnchangedTextualAnnotationType {
		a.hasChanges = true
	}
}

func (a *TextualAnnotations) MarkAdded(line string) {
	a.mark(line, AddedTextualAnnotationType)
}

func (a *TextualAnnotations) MarkRemoved(line string) {
	a.mark(line, RemovedTextualAnnotationType)
}

func (a *TextualAnnotations) MarkUnchanged(line string) {
	a.mark(line, UnchangedTextualAnnotationType)
}

func (a *TextualAnnotations) ByType(typ TextualAnnotationType) (r []*TextualAnnotatedLine) {
	for _, line := range a.lines {
		if line.typ == typ {
			r = append(r, line)
		}
	}
	return r
}

func (a *TextualAnnotations) Added() (r []*TextualAnnotatedLine) {
	return a.ByType(AddedTextualAnnotationType)
}

func (a *TextualAnnotations) Removed() (r []*TextualAnnotatedLine) {
	return a.ByType(RemovedTextualAnnotationType)
}

func (a *TextualAnnotations) Export(format TextualAnnotationFormat) string {
	textLines := make([]string, 0, len(a.lines))
	for _, annotatedLine := range a.lines {
		switch annotatedLine.typ {
		case AddedTextualAnnotationType:
			annotatedLine.line = "+" + annotatedLine.line
		case RemovedTextualAnnotationType:
			annotatedLine.line = "-" + annotatedLine.line
		default:
			// line unchanged
			switch format {
			case PlusMinusSpaceTextualAnnotationFormat:
				if a.hasChanges {
					annotatedLine.line = " " + annotatedLine.line
				}
			case PlusMinusEqualTextualAnnotationFormat:
				annotatedLine.line = "=" + annotatedLine.line
			case PlusMinusTextualAnnotationFormat:
			}
		}
		textLines = append(textLines, annotatedLine.line)
	}
	return strings.Join(textLines, "\n")
}

type TextualAnnotationFormat int

const (
	PlusMinusSpaceTextualAnnotationFormat TextualAnnotationFormat = iota
	PlusMinusEqualTextualAnnotationFormat
	PlusMinusTextualAnnotationFormat
	SchemadiffSuffixTextualAnnotationFormat
)

func annotatedStatement(stmt string, annotationType TextualAnnotationType, annotations *TextualAnnotations) *TextualAnnotations {
	stmtLines := strings.Split(stmt, "\n")
	result := NewTextualAnnotations()
	annotationLines := map[string]bool{}
	for _, annotation := range annotations.ByType(annotationType) {
		lines := strings.Split(annotation.line, "\n")
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
			fromLine := from.lines[fromIndex]
			if fromLine.typ == RemovedTextualAnnotationType {
				unified.MarkRemoved(fromLine.line)
				fromIndex++
				continue
			}
			matchingLine = fromLine.line
		}
		if toIndex < to.Len() {
			toLine := to.lines[toIndex]
			if toLine.typ == AddedTextualAnnotationType {
				unified.MarkAdded(toLine.line)
				toIndex++
				continue
			}
			if matchingLine == "" {
				matchingLine = toLine.line
			}
		}
		unified.MarkUnchanged(matchingLine)
		fromIndex++
		toIndex++
	}
	return unified
}
