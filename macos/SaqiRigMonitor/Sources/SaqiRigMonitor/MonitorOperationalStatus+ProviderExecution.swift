import Foundation

extension MonitorOperationalStatus {
    static func legacyRunningStatus(health: PipelineHealth, active: Int) -> Self {
        if let wait = health.enrichmentProviders.compactMap(\.wait).first(where: \.requiresAttention)
            ?? health.enrichmentProviders.compactMap(\.wait).first
        {
            return Self(
                label: active > 0 ? "Running · \(active) active · \(wait.action)" : wait.action,
                severity: wait.requiresAttention ? .attention : (active > 0 ? .healthy : .waiting),
                wait: wait
            )
        }
        if health.queues.contains(where: { $0.retryWait > 0 && $0.active == 0 }) {
            let wait = OperationalWait(
                action: "Retry wait; resuming automatically",
                kind: .retry,
                requiresAttention: false
            )
            return Self(
                label: active > 0 ? "Running · \(active) active · some work retrying" : wait.action,
                severity: active > 0 ? .healthy : .waiting,
                wait: wait
            )
        }
        if let check = health.attentionCheck {
            return Self(label: "\(check.code) · attention needed", severity: .attention, wait: nil)
        }
        return Self(
            label: active > 0 ? "Running · \(active) active" : "Running · healthy",
            severity: .healthy,
            wait: nil
        )
    }

    static func providerExecutionStatus(
        runtime: RuntimeStatus?,
        service: ServiceSnapshot,
        active: Int
    ) -> Self? {
        guard let runtime, runtime.matches(service: service),
              let provider = runtime.providerExecution?.providers.first(where: { $0.provider == "sol" }),
              provider.progress.readyWork > 0 || provider.progress.delayedWork > 0
              || provider.progress.activeInvocations > 0
        else { return nil }
        let reason = provider.admission.primaryReason
        if ["active", "ready", "no_ready_work"].contains(reason) {
            return nil
        }
        return providerStatus(provider, active: max(active, provider.progress.activeInvocations))
    }

    private static func providerStatus(_ provider: ProviderExecutionEntry, active: Int) -> Self? {
        let reason = provider.admission.primaryReason
        if reason == "auth_wait" {
            return authenticationStatus(provider, active: active)
        }
        if let blocking = blockingProviderStatus(provider) {
            return blocking
        }
        return automaticProviderStatus(reason, active: active)
    }

    private static func blockingProviderStatus(_ provider: ProviderExecutionEntry) -> Self? {
        switch provider.admission.primaryReason {
        case "operator_paused":
            providerWait("All work paused", action: "Resume all work", kind: .operatorPause, severity: .neutral)
        case "paid_work_paused":
            providerWait("Paid translation paused", action: "Resume paid work", kind: .paidPause, severity: .neutral)
        case "budget_unarmed":
            providerAttention("Translation budget not armed", action: "Arm a translation budget", kind: .budget)
        case "budget_exhausted":
            providerAttention("Translation budget exhausted", action: "Re-arm the translation budget", kind: .budget)
        case "circuit_open":
            providerAttention("Codex circuit open", action: "Inspect Codex provider", kind: .provider)
        case "disabled":
            providerAttention("Codex translation disabled", action: "Enable Codex translation", kind: .provider)
        default:
            nil
        }
    }

    private static func automaticProviderStatus(_ reason: String, active: Int) -> Self? {
        let values: [String: (String, OperationalWaitKind)] = [
            "codex_quota_wait": ("Codex quota reached", .quota),
            "error_dampener": ("Codex cooling down", .provider),
            "launch_pacing": ("Starting work gradually", .retry),
            "network_wait": ("Internet unavailable", .network),
            "provider_backoff": ("Codex cooling down", .provider),
            "rate_limit_wait": ("Codex rate limited", .rateLimit),
            "resource_wait": ("Resource pressure", .resource),
        ]
        guard let value = values[reason] else { return nil }
        return providerAutomatic(value.0, kind: value.1, active: active)
    }

    private static func authenticationStatus(_ provider: ProviderExecutionEntry, active: Int) -> Self {
        let requiresAttention = provider.admission.recovery == "operator"
            || provider.admission.operatorAction == "restore_auth"
        if requiresAttention {
            return providerAttention("Codex sign-in required", action: "Restore Codex sign-in", kind: .authentication)
        }
        return providerAutomatic("Codex authentication recovery", kind: .authentication, active: active)
    }

    private static func providerAttention(_ label: String, action: String, kind: OperationalWaitKind) -> Self {
        providerWait(
            "\(label) · attention needed",
            action: action,
            kind: kind,
            severity: .attention,
            attention: true
        )
    }

    private static func providerAutomatic(_ label: String, kind: OperationalWaitKind, active: Int) -> Self {
        providerWait(
            active > 0 ? "Running · \(active) active · \(label.lowercased())" : "\(label); resuming automatically",
            action: "\(label); resuming automatically",
            kind: kind,
            severity: active > 0 ? .healthy : .waiting
        )
    }

    private static func providerWait(
        _ label: String,
        action: String,
        kind: OperationalWaitKind,
        severity: MonitorSeverity,
        attention: Bool = false
    ) -> Self {
        Self(
            label: label,
            severity: severity,
            wait: OperationalWait(action: action, kind: kind, requiresAttention: attention)
        )
    }
}
