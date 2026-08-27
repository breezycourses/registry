{{- define "breezy-registry.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "breezy-registry.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "breezy-registry.labels" -}}
app.kubernetes.io/name: {{ include "breezy-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "breezy-registry.selectorLabels" -}}
app.kubernetes.io/name: {{ include "breezy-registry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "breezy-registry.headlessService" -}}
{{ include "breezy-registry.fullname" . }}-headless
{{- end -}}

{{/* In-cluster URL of pod $i, used for the shard list and per-pod self_url. */}}
{{- define "breezy-registry.podUrl" -}}
{{- $root := index . 0 -}}
{{- $i := index . 1 -}}
http://{{ include "breezy-registry.fullname" $root }}-{{ $i }}.{{ include "breezy-registry.headlessService" $root }}.{{ $root.Release.Namespace }}.svc.cluster.local:{{ $root.Values.service.port }}
{{- end -}}
