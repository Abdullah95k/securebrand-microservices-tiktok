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

{{/* "true" when the pod or hook named .name mounts secretFiles (the
proxies): those in secretFiles.components, or every one when the list is
empty. Takes (dict "root" $root "name" $name). The others start without
the Secret and never see the proxy credentials. */}}
{{- define "tks.secretFilesOn" -}}
{{- $sf := .root.Values.secretFiles -}}
{{- if and $sf.existingSecret (or (empty $sf.components) (has .name $sf.components)) -}}true{{- end -}}
{{- end -}}

{{- define "tks.configFiles" -}}
{{- if include "tks.secretFilesOn" . -}}
/etc/tks/config.yaml,/etc/tks-secret/{{ .root.Values.secretFiles.key }}
{{- else -}}
/etc/tks/config.yaml
{{- end -}}
{{- end -}}

{{/* Shared pod spec pieces: env, volumes, mounts.
tks.env takes a dict: root, and for a component its values (c); a hook
passes hook=true. */}}
{{- define "tks.env" -}}
{{- $root := .root -}}
{{- $c := .c | default dict -}}
- name: TKS_CONFIG
  value: {{ include "tks.configFiles" (dict "root" $root "name" .name) | quote }}
{{- if $root.Values.components.signer.enabled }}
# tks-signer's Service, for tt.signer.mode remote.
- name: TKS_TT__SIGNER__REMOTE_URL
  value: {{ printf "http://%s-signer:%v" (include "tks.fullname" $root) $root.Values.components.signer.port | quote }}
{{- end }}
{{- with $root.Values.platformSecret.name }}
{{- include "tks.platformEnv" (dict "root" $root "c" $c "hook" $.hook "secret" .) }}
{{- end }}
{{- end -}}

{{/* The production platform's Secret (platformSecret.name; M18) mapped
onto tks's settings. Each component logs in to Postgres as its own role
(c.postgresRole: POSTGRES_<ROLE>_USER/_PASSWORD), "none" gets no database
credentials; the API's read pool uses c.readRole; hooks log in as the owner
and grant every role its rights. */}}
{{- define "tks.platformEnv" -}}
{{- $s := .secret -}}
{{- $c := .c -}}
{{- $p := .root.Values.platformSecret -}}
{{- $env := list
  (list "TKS_KAFKA__BROKERS" "KAFKA_BROKERS" false)
  (list "TKS_KAFKA__SASL__MECHANISM" "KAFKA_SASL_MECHANISM" false)
  (list "TKS_KAFKA__SASL__USERNAME" "KAFKA_USER" false)
  (list "TKS_KAFKA__SASL__PASSWORD" "KAFKA_PASSWORD" false)
  (list "TKS_KAFKA__TLS__ENABLED" "KAFKA_TLS" true)
  (list "TKS_KAFKA__TLS__CA" "KAFKA_CA_CERT" true)
  (list "TKS_KAFKA__SCHEMA_REGISTRY__URL" "SCHEMA_REGISTRY_URL" true)
  (list "VALKEY_HOST" "VALKEY_HOST" false)
  (list "VALKEY_PORT" "VALKEY_PORT" false)
  (list "TKS_REDIS__PASSWORD" "VALKEY_PASSWORD" true)
  (list "TKS_REDIS__TLS__ENABLED" "VALKEY_TLS" true)
  (list "TKS_REDIS__TLS__CA" "VALKEY_CA_CERT" true)
  (list "TKS_S3__ENDPOINT" "S3_ENDPOINT" false)
  (list "TKS_S3__REGION" "S3_REGION" true)
  (list "TKS_S3__ACCESS_KEY" "S3_ACCESS_KEY" false)
  (list "TKS_S3__SECRET_KEY" "S3_SECRET_KEY" false)
  (list "TKS_S3__RAW_BUCKET" "S3_BUCKET_RAW" false)
  (list "TKS_S3__MEDIA_BUCKET" "S3_BUCKET_MEDIA" false)
  (list "TKS_S3__BACKUP_BUCKET" "S3_BUCKET_BACKUPS" true)
-}}
{{- $role := $c.postgresRole | default "" -}}
{{- if or .hook (ne $role "none") }}
{{- $env = concat $env (list
  (list "TKS_POSTGRES__HOST" "POSTGRES_HOST" false)
  (list "TKS_POSTGRES__PORT" "POSTGRES_PORT" true)
  (list "TKS_POSTGRES__DATABASE" "POSTGRES_DATABASE" false)
  (list "TKS_POSTGRES__SSLMODE" "POSTGRES_SSLMODE" true)
  (list "TKS_POSTGRES__CA" "POSTGRES_CA_CERT" true)) }}
{{- if or .hook (not $p.roles) (not $role) }}
{{- $env = concat $env (list (list "TKS_POSTGRES__USER" "POSTGRES_USER" false) (list "TKS_POSTGRES__PASSWORD" "POSTGRES_PASSWORD" false)) }}
{{- else }}
{{- $env = concat $env (list
  (list "TKS_POSTGRES__USER" (printf "POSTGRES_%s_USER" (upper $role)) false)
  (list "TKS_POSTGRES__PASSWORD" (printf "POSTGRES_%s_PASSWORD" (upper $role)) false)) }}
{{- with $c.readRole }}{{ if $p.roles }}
{{- $env = concat $env (list
  (list "TKS_POSTGRES__READ_USER" (printf "POSTGRES_%s_USER" (upper .)) false)
  (list "TKS_POSTGRES__READ_PASSWORD" (printf "POSTGRES_%s_PASSWORD" (upper .)) false)) }}
{{- end }}{{ end }}
{{- end }}
{{- if and .hook $p.roles }}
{{- range $r := list "api" "reader" "persister" "scheduler" "crawler" }}
{{- $env = append $env (list (printf "TKS_POSTGRES__ROLES__%s" (upper $r)) (printf "POSTGRES_%s_USER" (upper $r)) false) }}
{{- end }}
{{- end }}
{{- end }}
{{- range $env }}
- name: {{ index . 0 }}
  valueFrom: {secretKeyRef: {name: {{ $s }}, key: {{ index . 1 }}{{ if index . 2 }}, optional: true{{ end }}}}
{{- end }}
# Valkey's address from its parts (variables defined above).
- name: TKS_REDIS__ADDR
  value: "$(VALKEY_HOST):$(VALKEY_PORT)"
{{- end -}}

{{- define "tks.envFrom" -}}
{{- if .Values.secretEnv.existingSecret }}
envFrom:
  - secretRef:
      name: {{ .Values.secretEnv.existingSecret }}
{{- end }}
{{- end -}}

{{- define "tks.volumeMounts" -}}
{{- $root := .root -}}
- name: config
  mountPath: /etc/tks
  readOnly: true
{{- if include "tks.secretFilesOn" . }}
- name: secret-files
  mountPath: /etc/tks-secret
  readOnly: true
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  mountPath: {{ $root.Values.tlsFiles.mountPath }}
  readOnly: true
{{- end }}
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "tks.hookVolumes" -}}
{{- include "tks.volumesFor" (dict "root" .root "name" .name "configMap" (printf "%s-hook-config" (include "tks.fullname" .root))) -}}
{{- end -}}

{{- define "tks.volumesFor" -}}
{{- $root := .root -}}
- name: config
  configMap:
    name: {{ .configMap }}
{{- if include "tks.secretFilesOn" . }}
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
  {{- with .tmpMB }}
  emptyDir: {sizeLimit: {{ . }}Mi}
  {{- else }}
  emptyDir: {}
  {{- end }}
{{- end -}}
