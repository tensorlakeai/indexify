{{- define "labels" -}}
app.kubernetes.io/name: {{ .name | replace "-" "" }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/part-of: {{ .global.Chart.Name }}
app.kubernetes.io/instance: {{ .global.Release.Name }}
app.kubernetes.io/managed-by: {{ .global.Release.Service }}
{{- end }}

{{- define "blobStore.env" -}}
{{- with .blobStore -}}
{{- if .endpoint -}}
- name: AWS_ENDPOINT
  value: {{ .endpoint }}
- name: AWS_ENDPOINT_URL
  value: {{ .endpoint }}
{{- end }}
{{- if .allowHTTP }}
- name: AWS_ALLOW_HTTP
  value: "true"
{{- end }}
{{- if .credentialSecret }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .credentialSecret }}
      key: AWS_ACCESS_KEY_ID
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .credentialSecret }}
      key: AWS_SECRET_ACCESS_KEY
{{- end }}
{{- end }}
{{- end }}
