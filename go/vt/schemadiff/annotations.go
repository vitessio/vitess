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

var (
	annotationAddedHint   = " -- schemadiff:added"
	annotationRemovedHint = " -- schemadiff:removed"
)

func annotatedStatement(stmt string, annotationType TextualAnnotationType, annotationHint TextualAnnotationHint, annotations []string) string {
	anyLineAnnotated := false
	stmtLines := strings.Split(stmt, "\n")
	annotationLines := map[string]bool{}
	for _, annotation := range annotations {
		annotation = strings.TrimSpace(annotation)
		lines := strings.Split(annotation, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			annotationLines[line] = true
		}
	}
	for i := range stmtLines {
		lineAnnotated := false
		trimmedLine := stmtLines[i]
		trimmedLine = strings.TrimSpace(trimmedLine)
		// trimmedLine = strings.TrimRight(trimmedLine, ",")
		// trimmedLine = strings.TrimLeft(trimmedLine, "(")
		// trimmedLine = strings.TrimLeft(trimmedLine, ") ")
		// trimmedLine = strings.TrimSpace(trimmedLine)
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
				switch annotationHint {
				case PlusMinusSpaceTextualAnnotationHint,
					PlusMinusEqualTextualAnnotationHint,
					PlusMinusTextualAnnotationHint:
					switch annotationType {
					case AddedTextualAnnotationType:
						stmtLines[i] = "+" + stmtLines[i]
					case RemovedTextualAnnotationType:
						stmtLines[i] = "-" + stmtLines[i]
					}
				case SchemadiffSuffixTextualAnnotationHint:
					switch annotationType {
					case AddedTextualAnnotationType:
						stmtLines[i] = stmtLines[i] + annotationAddedHint
					case RemovedTextualAnnotationType:
						stmtLines[i] = stmtLines[i] + annotationRemovedHint
					}
				}
				lineAnnotated = true
				anyLineAnnotated = true
				delete(annotationLines, annotationLine) // no need to iterate it in the future
			}
		}
		if !lineAnnotated {
			switch annotationHint {
			case PlusMinusSpaceTextualAnnotationHint:
				stmtLines[i] = " " + stmtLines[i]
			case PlusMinusEqualTextualAnnotationHint:
				stmtLines[i] = "=" + stmtLines[i]
			case PlusMinusTextualAnnotationHint:
			// line unchanged
			case SchemadiffSuffixTextualAnnotationHint:
				// line unchanged
			}
		}
	}
	if !anyLineAnnotated {
		return stmt
	}
	return strings.Join(stmtLines, "\n")
}

func unifiedAnnotated(annotatedFrom string, annotatedTo string) string {
	fromLines := strings.Split(annotatedFrom, "\n")
	toLines := strings.Split(annotatedTo, "\n")
	unified := []string{}

	fromIndex := 0
	toIndex := 0
	for fromIndex < len(fromLines) || toIndex < len(toLines) {
		matchingLine := ""
		if fromIndex < len(fromLines) {
			fromLine := fromLines[fromIndex]
			if strings.HasSuffix(fromLine, annotationRemovedHint) {
				unified = append(unified, "- "+fromLine)
				fromIndex++
				continue
			}
			matchingLine = fromLine
		}
		if toIndex < len(toLines) {
			toLine := toLines[toIndex]
			if strings.HasSuffix(toLine, annotationAddedHint) {
				unified = append(unified, "+ "+toLine)
				toIndex++
				continue
			}
			if matchingLine == "" {
				matchingLine = toLine
			}
		}
		unified = append(unified, matchingLine)
		fromIndex++
		toIndex++
	}
	return strings.Join(unified, "\n")
}
