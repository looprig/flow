// Package ingress accepts inbound flow requests. See design docs/plans/2026-06-24-flow-engine-design.md §18.3.
//
// Authorization model — NO cross-run tenancy. The ingress has NO notion of
// run ownership or tenant isolation: any caller that passes WithAuth (or any
// caller at all, when no authenticator is configured) may resume, get, or cancel
// ANY run id. The only thing protecting one caller's run from another is the
// UNGUESSABILITY of the GraphRunID (a crypto/rand UUIDv4, §18.3) — there is no
// per-run access check. A deployment that needs real tenancy/authorization MUST
// enforce it in the WithAuth authenticator (e.g. derive the tenant from the
// request and reject ids outside it) or in a fronting proxy; the ingress will not
// do it for you. This is by design (CLAUDE.md: the auth policy is the caller's,
// wired at the composition root); a built-in ownership model is out of scope here.
package ingress
