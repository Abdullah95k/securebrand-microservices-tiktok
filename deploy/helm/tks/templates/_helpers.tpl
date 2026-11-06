{{- define "tks.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "tks.labels" -}}
app.kubernetes.io/name: tks
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "tks.selector" -}}
app.kubernetes.io/name: tks
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{- define "tks.configFiles" -}}
{{- if .Values.secretFiles.existingSecret -}}
/etc/tks/config.yaml,/etc/tks-secret/{{ .Values.secretFiles.key }}
{{- else -}}
/etc/tks/config.yaml
{{- end -}}
{{- end -}}

{{/* Shared pod spec pieces: env, volumes, mounts. */}}
{{- define "tks.env" -}}
- name: TKS_CONFIG
  value: {{ include "tks.configFiles" . | quote }}
{{- if .Values.components.signer.enabled }}
# tks-signer's Service, for tt.signer.mode remote.
- name: TKS_TT__SIGNER__REMOTE_URL
  value: {{ printf "http://%s-signer:%v" (include "tks.fullname" .) .Values.components.signer.port | quote }}
{{- end }}
{{- end -}}

{{- define "tks.envFrom" -}}
{{- if .Values.secretEnv.existingSecret }}
envFrom:
  - secretRef:
      name: {{ .Values.secretEnv.existingSecret }}
{{- end }}
{{- end -}}

{{- define "tks.volumeMounts" -}}
- name: config
  mountPath: /etc/tks
  readOnly: true
{{- if .Values.secretFiles.existingSecret }}
- name: secret-files
  mountPath: /etc/tks-secret
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

{{- define "tks.volumes" -}}
{{- include "tks.volumesFor" (dict "root" . "configMap" (printf "%s-config" (include "tks.fullname" .))) -}}
{{- end -}}

{{- define "tks.hookVolumes" -}}
{{- include "tks.volumesFor" (dict "root" . "configMap" (printf "%s-hook-config" (include "tks.fullname" .))) -}}
{{- end -}}

{{- define "tks.volumesFor" -}}
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
