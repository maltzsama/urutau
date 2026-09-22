{{/* The container image, defaulting the tag to the chart's appVersion. */}}
{{- define "urutau-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/* Standard labels. */}}
{{- define "urutau-operator.labels" -}}
app.kubernetes.io/name: urutau-operator
app.kubernetes.io/part-of: urutau
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}
