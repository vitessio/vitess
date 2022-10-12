/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
package viperget provides custom getter functions for retrieving values from a
viper.

For example, to retrieve a key as a topodatapb.TabletType variable, the
viperutil.Value should be instantiated like:

	// Passing `nil` to viperget.TabletType will have it lookup the key on the
	// current global viper.
	viperutil.NewValue[topodatapb.TabletType](myKey, viperget.TabletType(nil), opts...)
	// If using a specific viper instance, instead do the following.
	viperutil.NewValue[topodatapb.TabletType](myKey, viperget.TabletType(myViper), opts...)

This is a subpackage instead of being part of viperutil directly in order to
avoid an import cycle between go/vt/log and viperutil.
*/
package viperget
