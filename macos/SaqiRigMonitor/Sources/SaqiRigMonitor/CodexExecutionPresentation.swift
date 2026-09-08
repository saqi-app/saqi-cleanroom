import Foundation

internal struct CodexExecutionPresentation: Equatable {
    let accessibilityLabel: String
    let accessibilityValue: String
    let budget: String
    let concurrency: String
    let credentialSwitch: String?
    let detail: String?
    let headline: String
    let progress: String
}

private enum CodexTelemetryProvenance {
    case current
    case lastKnown
    case differentConfiguration

    func label(ageMinutes: Int) -> String? {
        switch self {
        case .current:
            nil
        case .lastKnown:
            "Last known · \(codexAgeLabel(ageMinutes))"
        case .differentConfiguration:
            "Different configuration · \(codexAgeLabel(ageMinutes))"
        }
    }
}

private func codexAgeLabel(_ minutes: Int) -> String {
    minutes == 1 ? "1 minute ago" : "\(minutes) minutes ago"
}

extension RigStore {
    private static func codexPresentation(
        provider: ProviderExecutionEntry,
        observedAt: Int,
        nowMilliseconds: Int,
        provenance: CodexTelemetryProvenance = .current
    ) -> CodexExecutionPresentation {
        let rawHeadline = codexHeadline(provider)
        let timing = codexTiming(provider, nowMilliseconds: nowMilliseconds)
        let action = codexAction(provider.admission.operatorAction)
        let rawDetail = [action, timing].compactMap(\.self).joined(separator: " · ")
        let rawProgress = "\(provider.progress.accepted) accepted · \(provider.progress.readyWork) ready · "
            + "\(provider.progress.delayedWork) delayed · \(provider.progress.terminalWork) terminal"
        let scheduler = provider.gates.scheduler
        let rawConcurrency = "\(scheduler.activeInvocations) active · selected \(scheduler.selectedConcurrency) · "
            + "ceiling \(scheduler.configuredConcurrency)"
        let rawBudget = codexBudget(provider.gates.budget)
        let ageMinutes = max(1, Int(ceil(Double(max(0, nowMilliseconds - observedAt)) / 60_000)))
        let provenanceLabel = provenance.label(ageMinutes: ageMinutes)
        let headline = provenanceLabel.map { "\($0) · \(rawHeadline)" } ?? rawHeadline
        let detail = rawDetail.isEmpty ? "" : labeled(rawDetail, provenanceLabel: provenanceLabel)
        let progress = labeled(rawProgress, provenanceLabel: provenanceLabel)
        let concurrency = labeled(rawConcurrency, provenanceLabel: provenanceLabel)
        let budget = labeled(rawBudget, provenanceLabel: provenanceLabel)
        let rawCredentialSwitch = provider.sessions.activePreviousAccountEpoch > 0
            ? "Credential switch · draining \(provider.sessions.activePreviousAccountEpoch) earlier sessions"
            : nil
        let credentialSwitch = rawCredentialSwitch.map { labeled($0, provenanceLabel: provenanceLabel) }
        let values = [headline, detail.isEmpty ? nil : detail, progress, concurrency, budget, credentialSwitch]
            .compactMap(\.self)
        return CodexExecutionPresentation(
            accessibilityLabel: "Codex translation status",
            accessibilityValue: values.joined(separator: ". "),
            budget: budget,
            concurrency: concurrency,
            credentialSwitch: credentialSwitch,
            detail: detail.isEmpty ? nil : detail,
            headline: headline,
            progress: progress
        )
    }

    private static func labeled(_ value: String, provenanceLabel: String?) -> String {
        provenanceLabel.map { "\($0) · \(value)" } ?? value
    }

    private static func codexHeadline(_ provider: ProviderExecutionEntry) -> String {
        if provider.admission.primaryReason == "active" {
            return "Translating · \(provider.progress.activeInvocations) active"
        }
        if provider.admission.primaryReason == "at_capacity" {
            return "At capacity · \(provider.progress.activeInvocations) active"
        }
        if provider.admission.primaryReason == "auth_wait" {
            return codexAuthenticationHeadline(provider.credentials)
        }
        return CodexCopy.headlines[provider.admission.primaryReason] ?? "Codex details unavailable"
    }

    private static func codexAuthenticationHeadline(_ credentials: ProviderCredentialHealth) -> String {
        CodexCopy.authenticationHeadlines[credentials.state] ?? "Codex authentication recovery"
    }

    private static func codexAction(_ action: String) -> String? {
        CodexCopy.actions[action]
    }

    private static func codexTiming(_ provider: ProviderExecutionEntry, nowMilliseconds: Int) -> String? {
        if provider.admission.primaryReason == "codex_quota_wait", let nextProbe = provider.gates.quota.nextProbeAt {
            return "Next probe \(relativeDeadline(nextProbe, nowMilliseconds: nowMilliseconds))"
        }
        guard let retryAt = provider.admission.retryAt else { return nil }
        let prefix = provider.admission.primaryReason == "launch_pacing" ? "Next launch" : "Retry"
        return "\(prefix) \(relativeDeadline(retryAt, nowMilliseconds: nowMilliseconds))"
    }

    private static func relativeDeadline(_ deadline: Int, nowMilliseconds: Int) -> String {
        guard deadline > nowMilliseconds else { return "due now" }
        let minutes = max(1, Int(ceil(Double(deadline - nowMilliseconds) / 60_000)))
        return minutes == 1 ? "in 1 minute" : "in \(minutes) minutes"
    }

    private static func codexBudget(_ budget: BudgetGate) -> String {
        guard budget.state != "unarmed" else { return "Budget · not armed" }
        return "Budget · \(budget.remainingOperations) of \(budget.maximumOperations) operations available · "
            + "\(budget.reservedOperations) reserved"
    }

    private static func unavailableCodexPresentation(_ headline: String) -> CodexExecutionPresentation {
        CodexExecutionPresentation(
            accessibilityLabel: "Codex translation status",
            accessibilityValue: headline,
            budget: "Budget unavailable",
            concurrency: "Concurrency unavailable",
            credentialSwitch: nil,
            detail: nil,
            headline: headline,
            progress: "Progress unavailable"
        )
    }

    private static func presentation(
        execution: ProviderExecutionHealth,
        nowMilliseconds: Int,
        provenance: CodexTelemetryProvenance
    ) -> CodexExecutionPresentation {
        let currentMaximumAge = 15 * 60_000
        let historicalMaximumAge = 60 * 60_000
        guard execution.observedAt <= nowMilliseconds + 30_000 else {
            return Self.unavailableCodexPresentation("Codex details unavailable · clock mismatch")
        }
        let age = nowMilliseconds - execution.observedAt
        guard age <= (provenance == .current ? currentMaximumAge : historicalMaximumAge) else {
            return Self.unavailableCodexPresentation(
                provenance == .current ? "Codex details stale" : "Codex historical details expired"
            )
        }
        guard let provider = execution.providers.first(where: { $0.provider == "sol" }) else {
            return Self.unavailableCodexPresentation("Codex details unavailable")
        }
        return Self.codexPresentation(
            provider: provider,
            observedAt: execution.observedAt,
            nowMilliseconds: nowMilliseconds,
            provenance: provenance
        )
    }
}

extension RigStore {
    func codexExecutionPresentation(now: Date = Date()) -> CodexExecutionPresentation? {
        let nowMilliseconds = Int(now.timeIntervalSince1970 * 1000)
        if let current = currentCodexPresentation(nowMilliseconds: nowMilliseconds) {
            return current
        }
        return retainedCodexPresentation(nowMilliseconds: nowMilliseconds)
    }

    private func currentCodexPresentation(nowMilliseconds: Int) -> CodexExecutionPresentation? {
        if let execution = providerExecutionHealth, let service, let runtime,
           runtime.matches(service: service),
           execution.configDigest == runtime.configDigest,
           execution.observedAt >= 0
        {
            let provenance: CodexTelemetryProvenance = runtimeRefreshFailed || serviceRefreshFailed
                ? .lastKnown : .current
            return Self.presentation(
                execution: execution,
                nowMilliseconds: nowMilliseconds,
                provenance: provenance
            )
        }
        return nil
    }

    private func retainedCodexPresentation(nowMilliseconds: Int) -> CodexExecutionPresentation? {
        // Runtime telemetry remains internally coherent even while launchd is
        // switching to a replacement configuration. It is useful historical
        // evidence, but must be labeled as non-current.
        if let execution = providerExecutionHealth, let runtime,
           execution.configDigest == runtime.configDigest,
           execution.observedAt >= 0
        {
            let provenance: CodexTelemetryProvenance = service == nil
                ? .lastKnown : .differentConfiguration
            return Self.presentation(
                execution: execution,
                nowMilliseconds: nowMilliseconds,
                provenance: provenance
            )
        }
        guard let retained = lastCoherentCodexTelemetry else {
            guard providerExecutionHealth != nil else { return nil }
            return Self.unavailableCodexPresentation("Codex details unavailable · configuration changed")
        }
        let provenance: CodexTelemetryProvenance = if let service,
                                                      !retained.runtime.matches(service: service)
        {
            .differentConfiguration
        } else {
            .lastKnown
        }
        return Self.presentation(
            execution: retained.execution,
            nowMilliseconds: nowMilliseconds,
            provenance: provenance
        )
    }
}

private enum CodexCopy {
    static let headlines = [
        "ready": "Ready to translate",
        "no_ready_work": "Caught up · waiting for poems",
        "adaptive_capacity": "Running at adaptive capacity",
        "launch_pacing": "Starting work gradually",
        "error_dampener": "Cooling down after errors",
        "operator_paused": "All work paused",
        "paid_work_paused": "Paid translation paused",
        "budget_unarmed": "Translation budget not armed",
        "budget_exhausted": "Translation budget exhausted",
        "resource_wait": "Resource pressure",
        "codex_quota_wait": "Codex quota reached",
        "network_wait": "Internet unavailable",
        "rate_limit_wait": "Codex rate limited",
        "provider_backoff": "Codex unavailable",
        "circuit_open": "Codex stopped after repeated failures",
        "disabled": "Codex translation disabled",
    ]

    static let authenticationHeadlines = [
        "absent": "Codex sign-in required",
        "observation_wait": "Checking Codex credentials",
        "material_verification": "Verifying refreshed Codex credentials",
        "account_switch_wait": "Switching Codex credentials",
        "account_verification": "Switching Codex credentials",
    ]

    static let actions = [
        "resume_all": "Resume all work",
        "resume_paid": "Resume paid work",
        "arm_budget": "Arm a translation budget",
        "rearm_budget": "Re-arm the translation budget",
        "restore_auth": "Restore Codex sign-in",
        "enable_provider": "Enable Codex translation",
        "inspect_provider": "Inspect Codex provider",
    ]
}
