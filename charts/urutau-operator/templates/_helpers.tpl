{{/* The container image, defaulting the tag to the chart's appVersion. */}}
{{- define "urutau-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/* The base name. It defaults to "urutau-operator" so an existing install
keeps its resource names; set nameOverride (or fullnameOverride) to run a
second release in the same cluster without colliding on the cluster-scoped
resources (ClusterRole, ClusterRoleBinding, ValidatingWebhookConfiguration). */}}
{{- define "urutau-operator.name" -}}
{{- default "urutau-operator" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The full name: fullnameOverride when set, else name. */}}
{{- define "urutau-operator.fullname" -}}
{{- default (include "urutau-operator.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Standard labels. */}}
{{- define "urutau-operator.labels" -}}
app.kubernetes.io/name: {{ include "urutau-operator.name" . }}
app.kubernetes.io/part-of: urutau
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{/* Selector labels — the immutable subset the Deployment/Service/PDB select
on. Kept separate from the full label set so a future mutable label cannot
leak into a selector. */}}
{{- define "urutau-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "urutau-operator.name" . }}
control-plane: controller-manager
{{- end -}}
