/*
Copyright 2019 The Vitess Authors.

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

//Package visitorgen is responsible for taking the ast.go of Vitess and
//and producing visitor infrastructure for it.
//
//This is accomplished in a few steps.
//Step 1:	Walk the AST and collect the interesting information into a format that is
//       	easy to consume for the next step. The output format is a *SourceFile, that
//      	contains the needed information in a format that is pretty close to the golang ast,
//     		but simplified
//Step 2:	A SourceFile is packaged into a SourceInformation. SourceInformation is still
//			concerned with the input ast - it's just an even more distilled and easy to
//			consume format for the last step. This step is performed by the code in transformer.go.
//Step 3:	Using the SourceInformation, the struct_producer.go code produces the final data structure
//			used, a VisitorPlan. This is focused on the output - it contains a list of all fields or
//			arrays that need to be handled by the visitor produced.
//Step 4:	The VisitorPlan is lastly turned into a string that is written as the output of
//			this whole process.
package visitorgen
