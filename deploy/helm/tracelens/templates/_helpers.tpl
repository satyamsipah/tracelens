{{/* Chart-wide naming and label helpers. */}}

{{- define "tracelens.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tracelens.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "tracelens.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tracelens.labels" -}}
helm.sh/chart: {{ include "tracelens.chart" . }}
{{ include "tracelens.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: tracelens
{{- end -}}

{{- define "tracelens.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tracelens.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "tracelens.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tracelens.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference for one component. Components share a repository prefix and
differ only by suffix, so a release pins one tag across the whole platform --
mixing versions between the collector and the assembler is a real way to get
a schema/protocol mismatch.
*/}}
{{- define "tracelens.image" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- $tag := default $root.Chart.AppVersion $root.Values.image.tag -}}
{{- printf "%s/%s-%s:%s" $root.Values.image.registry $root.Values.image.repository $component $tag -}}
{{- end -}}

{{/* Name of the Secret holding the ClickHouse password. */}}
{{- define "tracelens.clickhouseSecretName" -}}
{{- if .Values.clickhouse.existingSecret -}}
{{- .Values.clickhouse.existingSecret -}}
{{- else -}}
{{- printf "%s-clickhouse" (include "tracelens.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Environment shared by every component that talks to ClickHouse. The password
always arrives via secretKeyRef -- it is never rendered into a manifest,
where it would land in `kubectl get -o yaml`, in Helm's release history, and
in whatever GitOps repo holds the rendered output.
*/}}
{{- define "tracelens.clickhouseEnv" -}}
- name: TRACELENS_CLICKHOUSE_ADDR
  value: {{ .Values.clickhouse.addr | quote }}
- name: TRACELENS_CLICKHOUSE_DB
  value: {{ .Values.clickhouse.database | quote }}
- name: TRACELENS_CLICKHOUSE_USER
  value: {{ .Values.clickhouse.user | quote }}
- name: TRACELENS_CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "tracelens.clickhouseSecretName" . }}
      key: {{ .Values.clickhouse.existingSecretPasswordKey }}
{{- end -}}

{{- define "tracelens.kafkaEnv" -}}
- name: TRACELENS_KAFKA_BROKERS
  value: {{ .Values.kafka.brokers | quote }}
- name: TRACELENS_KAFKA_PARTITIONS
  value: {{ .Values.kafka.partitions | quote }}
- name: TRACELENS_TOPIC_SPANS
  value: {{ .Values.kafka.topics.spans | quote }}
- name: TRACELENS_TOPIC_LOGS
  value: {{ .Values.kafka.topics.logs | quote }}
- name: TRACELENS_TOPIC_METRICS
  value: {{ .Values.kafka.topics.metrics | quote }}
{{- end -}}
