package controlplane

// Typed errors for the in-process control plane (§18.5). Per CLAUDE.md every
// package-level failure mode is a concrete error struct (pointer receiver) so a
// caller can errors.As to inspect it, rather than a bare errors.New/fmt.Errorf.

// ClosedError reports an operation attempted after the control plane has been
// shut down (its lifetime ctx cancelled via Close). It is fail-secure: a
// shut-down control plane rejects new work rather than silently dropping it
// (§18.5). It carries no fields — the failure is context-free (the plane is
// simply gone) — but remains a typed struct so callers can errors.As for it.
type ClosedError struct{}

// Error implements error.
func (e *ClosedError) Error() string {
	return "controlplane: control plane is closed"
}
