/*
Copyright 2023 The Vitess Authors.

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

package common

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestParseAndValidateCreateOptions(t *testing.T) {
	tests := []struct {
		name    string
		setFunc func(*cobra.Command) error
		wantErr bool
	}{
		{
			name: "invalid tablet type",
			setFunc: func(cmd *cobra.Command) error {
				tabletTypesFlag := cmd.Flags().Lookup("tablet-types")
				err := tabletTypesFlag.Value.Set("invalid")
				tabletTypesFlag.Changed = true
				return err
			},
			wantErr: true,
		},
		{
			name: "no tablet types",
			setFunc: func(cmd *cobra.Command) error {
				tabletTypesFlag := cmd.Flags().Lookup("tablet-types")
				err := tabletTypesFlag.Value.Set("")
				tabletTypesFlag.Changed = true
				return err
			},
			wantErr: true,
		},
		{
			name: "valid tablet types",
			setFunc: func(cmd *cobra.Command) error {
				tabletTypesFlag := cmd.Flags().Lookup("tablet-types")
				err := tabletTypesFlag.Value.Set("rdonly,replica")
				tabletTypesFlag.Changed = true
				return err
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			AddCommonCreateFlags(cmd)
			test := func() error {
				if tt.setFunc != nil {
					if err := tt.setFunc(cmd); err != nil {
						return err
					}
				}
				if err := ParseAndValidateCreateOptions(cmd); err != nil {
					return err
				}
				return nil
			}
			if err := test(); (err != nil) != tt.wantErr {
				t.Errorf("ParseAndValidateCreateOptions() error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}
