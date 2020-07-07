package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VitessTopoNode is a container for Vitess topology data
type VitessTopoNode struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Data              VitessTopoNodeData `json:"data"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VitessTopoNodeList is a top-level list type. The client methods for lists are automatically created.
type VitessTopoNodeList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []VitessTopoNode `json:"items"`
}

// VitessTopoNodeData contains the basic data for the node
type VitessTopoNodeData struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Ephemeral bool   `json:"ephemeral"`
}
