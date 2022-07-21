/*
Copyright 2021 The Vitess Authors.

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

/*
Package workflow defines types and functions for working with Vitess workflows.

This is still a very rough sketch, far from a final API, but I want to document
some things here as I go:

(1) The lines between package workflow and package workflow/vexec are, uh,
	blurry at best, and definitely need serious thinking and refinement. Maybe
	there shouldn't even be two separate packages at all. The reason I have the
	two packages right now is because I'm operating under the assumption that
	there are workflows that are vexec, and then there are other workflows. If
	it's true that all workflows are vexec workflows, then probably one single
	package could make more sense. For now, two packages seems the way to go,
	but like I said, the boundaries are blurry, and things that belong in one
	package are in the other, because I haven't gone back and moved things
	around.
(2) I'm aiming for this to be a drop-in replacement (more or less) for the
	function calls in go/vt/wrangler. However, I'd rather define a better
	abstraction if it means having to rewrite even significant portions of the
	existing wrangler code to adapt to it, than make a subpar API in the name of
	backwards compatibility. I'm not sure if that's a tradeoff I'll even need to
	consider in the future, but I'm putting a stake in the ground on which side
	of that tradeoff I intend to fall, should it come to it.
(3) Eventually we'll need to consider how the online schema migration workflows
	fit into this. I'm trying to at least be somewhat abstract in the
	vexec / queryplanner APIs to fit with the QueryParams thing that wrangler
	uses, which _should_ work, but who knows?? Time will tell.
*/
package workflow
