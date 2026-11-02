{{- define "voting.name" -}}
{{- printf "%s-voting" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- define "voting.labels" -}}
app.kubernetes.io/name: voting
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
{{- define "voting.image" -}}
{{ .Values.image.repository }}:{{ required "image.tag must be an immutable commit SHA or version" .Values.image.tag }}
{{- end -}}

{{- define "voting.dnsEgress" -}}
- to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: {{ .Values.networkPolicy.dnsNamespace }}
    {{- range .Values.networkPolicy.dnsCIDRs }}
    - ipBlock: {cidr: {{ . | quote }}}
    {{- end }}
  ports: [{port: 53, protocol: UDP}, {port: 53, protocol: TCP}]
{{- end -}}
