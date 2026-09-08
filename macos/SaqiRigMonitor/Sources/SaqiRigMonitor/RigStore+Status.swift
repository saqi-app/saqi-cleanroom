import AppKit
import Foundation

extension RigStore {
    func aggregateTelemetryLabel(now: Date = Date()) -> String? {
        guard let health else { return nil }
        let age = now.timeIntervalSince1970 - Double(health.observedAt) / 1000
        let minutes = max(1, Int(ceil(max(0, age) / 60)))
        let ageLabel = minutes == 1 ? "1 minute ago" : "\(minutes) minutes ago"
        if service != nil, !identityMatches {
            return "Different configuration · \(ageLabel)"
        }
        let maximumAge = Double(max(15 * 60_000, (health.heartbeatIntervalMs ?? 5 * 60_000) * 3)) / 1000
        if healthRefreshFailed || serviceRefreshFailed || service == nil || age < -30 || age > maximumAge {
            return "Last known · \(ageLabel)"
        }
        return nil
    }
}

extension RigStore {
    var isFresh: Bool {
        isRecent && identityMatches
    }

    var isRecent: Bool {
        guard let health else { return false }
        let age = Date().timeIntervalSince1970 - Double(health.observedAt) / 1000
        let maximumAge = Double(max(15 * 60_000, (health.heartbeatIntervalMs ?? 5 * 60_000) * 3)) / 1000
        return age >= -30 && age <= maximumAge
    }

    var identityMatches: Bool {
        guard let health, let service else { return false }
        return service.configDigest == health.configDigest
            && service.ownerPid.map { health.runId == "process:\($0)" } == true
    }

    var acceptedPerHour: Double? {
        guard let latest = samples.last else { return nil }
        let age = Date().timeIntervalSince(latest.observedAt)
        guard age >= -30, age <= 60 * 60
        else { return nil }
        let rates = providerRates
        guard !rates.isEmpty else { return nil }
        return rates.values.reduce(0, +)
    }

    var operationalStatus: MonitorOperationalStatus {
        MonitorOperationalStatus.evaluate(
            service: serviceRefreshFailed ? nil : service,
            health: healthRefreshFailed ? nil : health,
            runtime: runtimeRefreshFailed || serviceRefreshFailed ? nil : runtime,
            paused: paused
        )
    }

    var serviceControlsAvailable: Bool {
        guard !healthRefreshFailed, !runtimeRefreshFailed, !serviceRefreshFailed,
              let service
        else { return false }
        if let runtime, !runtime.matches(service: service) {
            return false
        }
        if let health {
            guard service.configDigest == health.configDigest,
                  service.ownerPid.map({ health.runId == "process:\($0)" }) == true
            else { return false }
        }
        return true
    }

    var concurrencyControlsAvailable: Bool {
        guard !healthRefreshFailed, !runtimeRefreshFailed, !serviceRefreshFailed,
              let health, let runtime, let service,
              runtime.matches(service: service)
        else { return false }
        return service.configDigest == health.configDigest
            && service.ownerPid.map { health.runId == "process:\($0)" } == true
    }

    var eta: String {
        guard !serviceRefreshFailed, let service else { return "ETA unavailable" }
        if service.actualState == "stopped" {
            return "ETA stopped"
        }
        if service.actualState == "fenced" {
            return "ETA blocked"
        }
        if service.actualState == "running_outdated" {
            if service.onlySourceVerificationUnavailable {
                return "ETA waiting for source identity verification"
            }
            return "ETA restart required"
        }
        if service.actualState == "starting" {
            return "ETA starting"
        }
        guard service.actualState == "running" else { return "ETA unavailable" }
        guard !paused else { return "ETA paused" }
        guard !healthRefreshFailed else {
            if let provider = currentSolProviderExecution(),
               let estimate = ProgressEstimator.eta(provider: provider)
            {
                return estimate
            }
            return "ETA unavailable"
        }
        if let provider = currentSolProviderExecution(),
           MonitorOperationalStatus.providerExecutionStatus(
               runtime: runtime,
               service: service,
               active: provider.progress.activeInvocations
           )?.wait?.requiresAttention == true,
           let estimate = ProgressEstimator.eta(provider: provider)
        {
            return estimate
        }
        if isFresh, health?.growth?.production.state == "stalled" {
            return "ETA unavailable · production stalled"
        }
        if isFresh, health?.state == "blocked"
            || health?.checks.contains(where: { $0.state == "blocked" }) == true
        {
            return "ETA blocked"
        }
        if let provider = currentSolProviderExecution(),
           let estimate = ProgressEstimator.eta(provider: provider)
        {
            return estimate
        }
        guard !healthRefreshFailed, isFresh else { return "ETA unavailable" }
        let estimate = ProgressEstimator.eta(health: health, rates: providerRates)
        if estimate == "ETA learning" {
            return "ETA learning · \(providerRates.count)/\(health?.enrichmentProviders.count ?? 0) lanes"
        }
        return estimate
    }

    var rateLabel: String {
        let currentService = serviceRefreshFailed ? nil : service
        let currentPublished = currentPublishedRateLabel
        let recentAccepted = recentAcceptedRateLabel
        let lastMeasured = currentPublished ?? recentAccepted
        if !healthRefreshFailed, isFresh, health?.growth?.production.state == "stalled" {
            return lastMeasured.map { "Last measured · \($0) · stalled" } ?? "Rate unavailable · stalled"
        }
        if currentService?.actualState == "fenced" || (!healthRefreshFailed && isFresh && (health?.state == "blocked"
                || health?.checks.contains(where: { $0.state == "blocked" }) == true))
        {
            return lastMeasured.map { "Last measured · \($0) · blocked" } ?? "Rate unavailable · blocked"
        }
        if currentService?.actualState == "running_outdated" {
            if currentService?.onlySourceVerificationUnavailable == true {
                return lastMeasured.map { "Last measured · \($0) · source identity unverified" }
                    ?? "Rate unavailable · source identity unverified"
            }
            return lastMeasured.map { "Last measured · \($0) · restart required" }
                ?? "Rate unavailable · restart required"
        }
        if currentService?.actualState == "stopped" {
            return recentAccepted.map { "Last measured · \($0) · stopped" } ?? "Rate unavailable · stopped"
        }
        if currentService?.actualState == "starting" {
            return "Rate unavailable · starting"
        }
        if paused {
            return lastMeasured.map { "Last measured · \($0) · paused" } ?? "Rate paused"
        }
        if let currentPublished {
            return currentPublished
        }
        if healthRefreshFailed || !isFresh {
            if let recentAccepted {
                return "Last measured · \(recentAccepted)"
            }
            return isRecent ? "Rate unavailable · starting" : "Rate unavailable · stale"
        }
        if let recentAccepted {
            return recentAccepted
        }
        return "Learning · 0/\(health?.enrichmentProviders.count ?? 0) lanes ready"
    }

    private var currentPublishedRateLabel: String? {
        guard let provider = currentSolProviderExecution(), let throughput = provider.throughput else { return nil }
        return "\(throughput.published.last1h)/hour published · rolling 1h"
    }

    private var recentAcceptedRateLabel: String? {
        guard let rate = acceptedPerHour else { return nil }
        let known = providerRates.count
        let total = healthRefreshFailed ? known : health?.enrichmentProviders.count ?? known
        let measuredRate = rate.formatted(.number.precision(.fractionLength(0)))
        return "\(measuredRate)/hour accepted · \(known)/\(total) lanes measured"
    }

    func configuredConcurrency(_ provider: String) -> Int? {
        guard concurrencyControlsAvailable else { return nil }
        return health?.enrichmentProviders.first { $0.provider == provider }?.invocationConcurrency
    }

    func displayedConcurrency(_ provider: String) -> Int? {
        requestedConcurrency[provider] ?? configuredConcurrency(provider)
    }

    func concurrencyControlLabel(_ provider: String) -> String {
        Self.concurrencyControlLabel(
            provider: provider,
            configured: configuredConcurrency(provider),
            requested: requestedConcurrency[provider],
            state: concurrencyApplicationState[provider]
        )
    }

    private func currentSolProviderExecution(now: Date = Date()) -> ProviderExecutionEntry? {
        guard !runtimeRefreshFailed, !serviceRefreshFailed,
              let execution = providerExecutionHealth, let runtime, let service,
              runtime.matches(service: service),
              execution.configDigest == runtime.configDigest
        else { return nil }
        let nowMilliseconds = Int(now.timeIntervalSince1970 * 1000)
        guard execution.observedAt <= nowMilliseconds + 30_000,
              nowMilliseconds - execution.observedAt <= 15 * 60_000
        else { return nil }
        return execution.providers.first { $0.provider == "sol" }
    }
}
