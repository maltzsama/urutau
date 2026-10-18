// +groupName=urutau.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the CR group/version.
var GroupVersion = schema.GroupVersion{Group: "urutau.io", Version: "v1alpha1"}

// SchemeBuilder adds the CR types to a scheme. It uses the apimachinery
// builder so the api package keeps only its apimachinery dependencies
// (controller-runtime's scheme.Builder is deprecated for api packages).
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds the types to a scheme.
func AddToScheme(s *runtime.Scheme) error {
	return SchemeBuilder.AddToScheme(s)
}

// addKnownTypes registers the CR types under GroupVersion, mirroring what
// the deprecated controller-runtime builder did (types + group version meta).
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &CDCPipeline{}, &CDCPipelineList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
