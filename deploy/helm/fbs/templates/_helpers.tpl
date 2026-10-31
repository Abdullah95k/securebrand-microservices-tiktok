{{- define "fbs.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "fbs.labels" -}}
app.kubernetes.io/name: fbs
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "fbs.selector" -}}
app.kubernetes.io/name: fbs
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{- define "fbs.configFiles" -}}
{{- if .Values.secretFiles.existingSecret -}}
/etc/fbs/config.yaml,/etc/fbs-secret/{{ .Values.secretFiles.key }}
{{- else -}}
/etc/fbs/config.yaml
{{- end -}}
{{- end -}}

{{/* Shared pod spec pieces: env, volumes, mounts. */}}
{{- define "fbs.env" -}}
- name: IGS_CONFIG
  value: {{ include "fbs.configFiles" . | quote }}
{{- end -}}

{{- define "fbs.envFrom" -}}
{{- if .Values.secretEnv.existingSecret }}
envFrom:
  - secretRef:
      name: {{ .Values.secretEnv.existingSecret }}
{{- end }}
{{- end -}}

{{- define "fbs.volumeMounts" -}}
- name: config
  mountPath: /etc/fbs
  readOnly: true
{{- if .Values.secretFiles.existingSecret }}
- name: secret-files
  mountPath: /etc/fbs-secret
  readOnly: true
{{- end }}
{{- if .Values.tlsFiles.existingSecret }}
- name: tls-files
  mountPath: {{ .Values.tlsFiles.mountPath }}
  readOnly: true
{{- end }}
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "fbs.volumes" -}}
{{- include "fbs.volumesFor" (dict "root" . "configMap" (printf "%s-config" (include "fbs.fullname" .))) -}}
{{- end -}}

{{- define "fbs.hookVolumes" -}}
{{- include "fbs.volumesFor" (dict "root" . "configMap" (printf "%s-hook-config" (include "fbs.fullname" .))) -}}
{{- end -}}

{{- define "fbs.volumesFor" -}}
{{- $root := .root -}}
- name: config
  configMap:
    name: {{ .configMap }}
{{- if $root.Values.secretFiles.existingSecret }}
- name: secret-files
  secret:
    secretName: {{ $root.Values.secretFiles.existingSecret }}
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  secret:
    secretName: {{ $root.Values.tlsFiles.existingSecret }}
{{- end }}
- name: tmp
  emptyDir: {}
{{- end -}}
