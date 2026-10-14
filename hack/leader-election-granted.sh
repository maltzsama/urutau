# leader_election_granted FILE [NAMESPACE] succeeds when the rendered
# manifests in FILE hold, in NAMESPACE (default urutau-system), a Role that
# grants coordination.k8s.io leases and a RoleBinding to a Role. The operator
# takes its leader-election Lease in its own namespace, which a
# namespace-scoped install does not otherwise bind (#867).
leader_election_granted() {
  awk -v ns="${2:-urutau-system}" '
    function flush() {
      if (doc ~ /\nkind: Role\n/ && index(doc, "\n  namespace: " ns "\n") &&
          doc ~ /coordination\.k8s\.io/ && doc ~ /leases/) role = 1
      if (doc ~ /\nkind: RoleBinding\n/ && index(doc, "\n  namespace: " ns "\n") &&
          doc ~ /\n  kind: Role\n/) bind = 1
      doc = "\n"
    }
    BEGIN { doc = "\n" }
    /^---/ { flush(); next }
    { doc = doc $0 "\n" }
    END { flush(); exit !(role && bind) }
  ' "$1"
}
