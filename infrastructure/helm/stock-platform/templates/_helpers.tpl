{{/*
Common labels
*/}}
{{- define "stock-platform.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Namespace
*/}}
{{- define "stock-platform.namespace" -}}
{{ .Values.global.namespace | default .Release.Namespace }}
{{- end }}

{{/*
Image with registry
*/}}
{{- define "stock-platform.image" -}}
{{ .registry }}/{{ .repository }}:{{ .tag }}
{{- end }}

{{/*
Common environment variables
*/}}
{{- define "stock-platform.commonEnv" -}}
- name: DB_HOST
  valueFrom:
    configMapKeyRef:
      name: stock-platform-config
      key: DB_HOST
- name: DB_PORT
  valueFrom:
    configMapKeyRef:
      name: stock-platform-config
      key: DB_PORT
- name: DB_NAME
  valueFrom:
    secretKeyRef:
      name: stock-platform-secrets
      key: DB_NAME
- name: DB_USER
  valueFrom:
    secretKeyRef:
      name: stock-platform-secrets
      key: DB_USER
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: stock-platform-secrets
      key: DB_PASSWORD
- name: KAFKA_BOOTSTRAP_SERVERS
  valueFrom:
    configMapKeyRef:
      name: stock-platform-config
      key: KAFKA_BOOTSTRAP_SERVERS
- name: KAFKA_TOPIC_SECOND
  valueFrom:
    configMapKeyRef:
      name: stock-platform-config
      key: KAFKA_TOPIC_SECOND
- name: KAFKA_TOPIC_MINUTE
  valueFrom:
    configMapKeyRef:
      name: stock-platform-config
      key: KAFKA_TOPIC_MINUTE
- name: MASSIVE_API_KEY
  valueFrom:
    secretKeyRef:
      name: stock-platform-secrets
      key: MASSIVE_API_KEY
{{- end }}
