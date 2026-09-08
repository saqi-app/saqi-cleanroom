import SwiftUI

internal struct MonitorView: View {
    @ObservedObject var store: RigStore

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            header
            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    AggregateTelemetryView(label: store.aggregateTelemetryLabel()) {
                        ProductionStatusView(health: store.health)
                        integrityStatus
                        attentionStatus
                        Divider()
                        QueueLaneView(title: "Collection", queue: store.health?.collectionPoemQueue, detail: "poems")
                        QueueLaneView(title: "Authors", queue: store.health?.collectionAuthorQueue, detail: "authors")
                    }
                    operationalWait
                    Divider()
                    enrichmentLanes
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            Divider()
            controls
            footer
        }
        .padding(16)
        .frame(width: 360)
        .frame(maxHeight: 720)
    }

    @ViewBuilder private var integrityStatus: some View {
        if let conflict = store.health?.duplicateConflict {
            VStack(alignment: .leading, spacing: 3) {
                Label("Integrity · duplicate rejected", systemImage: "exclamationmark.shield.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.red)
                Text(conflict.detail ?? conflict.code)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
    }

    @ViewBuilder private var attentionStatus: some View {
        if let check = store.health?.attentionCheck {
            VStack(alignment: .leading, spacing: 3) {
                Label(
                    check.state == "blocked" ? "Blocked · \(check.code)" : "Notice · \(check.code)",
                    systemImage: "exclamationmark.triangle.fill"
                )
                .font(.caption.weight(.semibold))
                .foregroundStyle(.orange)
                if let detail = check.detail {
                    Text(detail).font(.caption2).foregroundStyle(.secondary)
                }
            }
        }
    }

    @ViewBuilder private var operationalWait: some View {
        if let wait = store.operationalStatus.wait, !wait.requiresAttention {
            Label(wait.action, systemImage: "arrow.clockwise.circle.fill")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.blue)
        }
    }

    private var header: some View {
        HStack(alignment: .top) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Saqi").font(.system(.title2, design: .serif, weight: .semibold))
                RefreshDiagnosticsStatus(freshnessLabel: updatedLabel, outcome: store.refreshOutcome)
            }
            Spacer()
            VStack(alignment: .trailing, spacing: 4) {
                RefreshDiagnosticsButton(store: store)
                HStack(spacing: 4) {
                    if store.operationalStatus.requiresAttention {
                        Image(systemName: "exclamationmark.circle.fill")
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.red)
                            .accessibilityLabel("Pipeline attention required")
                    }
                    Label(store.operationalStatus.label, systemImage: statusSymbol)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(statusColor)
                }
            }
        }
    }

    private var enrichmentLanes: some View {
        VStack(alignment: .leading, spacing: 5) {
            if let presentation = store.codexExecutionPresentation() {
                CodexExecutionCard(presentation: presentation)
            }
            AggregateTelemetryView(label: store.aggregateTelemetryLabel()) {
                ForEach(store.codexEnrichmentProviders) { provider in
                    HStack {
                        Text("\(provider.provider.uppercased()) · \(provider.model)")
                            .font(.caption2.weight(.semibold)).foregroundStyle(.secondary)
                        Spacer()
                        Text(provider.activityLabel())
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    QueueLaneView(
                        title: "Reviewed locally",
                        queue: store.health?.queue(provider.workKind),
                        detail: "poems"
                    )
                }
                if let host = store.health?.providerHostAdmission {
                    Text(host.label)
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.secondary)
                }
            }
            LocalProgressView(rateLabel: store.rateLabel, eta: store.eta)
        }
    }

    private var controls: some View {
        VStack(alignment: .leading, spacing: 8) {
            serviceControls
            concurrencyControls
            secondaryControls
        }
        .buttonStyle(.bordered)
        .disabled(store.phase.isApplying)
    }

    private var concurrencyControls: some View {
        HStack {
            CodexConcurrencyMenu(store: store)
        }
        .disabled(!store.concurrencyControlsAvailable)
    }

    private var secondaryControls: some View {
        HStack {
            if store.paused {
                Button("Resume", action: store.resume)
            } else {
                Button("Pause after current work", action: store.pauseAfterCurrentWork)
            }
            Menu("More") {
                Toggle(
                    "Start at login",
                    isOn: Binding(
                        get: { store.startsAtLogin },
                        set: { enabled in store.setStartsAtLogin(enabled) }
                    )
                )
                Button("Copy diagnostics", action: store.copyDiagnostics)
                Button("Open logs", action: store.openLogs)
            }
        }
    }

    private var footer: some View {
        HStack {
            if let label = store.phase.label {
                Text(label).foregroundStyle(.secondary)
            }
            Spacer()
            Text("Local progress").foregroundStyle(.secondary).accessibilityHidden(true)
        }
        .font(.caption)
    }

    private var updatedLabel: String {
        guard let observedAt = store.health?.observedAt else { return "Waiting for diagnostics" }
        let date = Date(timeIntervalSince1970: Double(observedAt) / 1000)
        return "Updated \(date.formatted(.relative(presentation: .numeric)))"
    }

    private var statusSymbol: String {
        switch store.operationalStatus.severity {
        case .attention:
            "exclamationmark.triangle.fill"
        case .healthy:
            "checkmark.circle.fill"
        case .neutral:
            store.paused ? "pause.circle.fill" : "stop.circle.fill"
        case .waiting:
            "arrow.clockwise.circle.fill"
        }
    }

    private var statusColor: Color {
        switch store.operationalStatus.severity {
        case .attention:
            .orange
        case .healthy:
            .green
        case .neutral:
            .secondary
        case .waiting:
            .blue
        }
    }
}

internal struct RefreshDiagnosticsStatus: View {
    let freshnessLabel: String
    let outcome: String?

    var historicalOutcomeLabel: String? {
        outcome.map { "Last manual refresh · \($0)" }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(freshnessLabel).font(.caption).foregroundStyle(.secondary)
            if let historicalOutcomeLabel {
                Text(historicalOutcomeLabel).font(.caption2).foregroundStyle(.secondary)
            }
        }
    }
}

private struct RefreshDiagnosticsButton: View {
    @ObservedObject var store: RigStore

    var body: some View {
        Button {
            Task { await store.refreshDiagnostics() }
        } label: {
            HStack(spacing: 4) {
                if store.refreshingDiagnostics || store.activeRefreshes > 0 {
                    ProgressView().controlSize(.mini)
                } else {
                    Image(systemName: "arrow.clockwise")
                }
                Text(store.refreshDiagnosticsTitle)
            }
        }
        .buttonStyle(.borderless)
        .disabled(!store.canRefreshDiagnostics)
        .help("Retry diagnostics and local progress history. Does not start, resume, or restart work.")
        .accessibilityLabel("\(store.refreshDiagnosticsTitle) diagnostics")
    }
}

private struct AggregateTelemetryView<Content: View>: View {
    let label: String?
    @ViewBuilder let content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 5) {
            if let label {
                Text(label).font(.caption.weight(.semibold)).foregroundStyle(.secondary)
            }
            content.opacity(label == nil ? 1 : 0.6)
        }
    }
}

private struct LocalProgressView: View {
    let rateLabel: String
    let eta: String

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text("Local · \(rateLabel)")
            HStack {
                Spacer()
                Text(eta)
            }
        }
        .font(.caption)
        .foregroundStyle(.secondary)
    }
}

private struct ProductionStatusView: View {
    let health: PipelineHealth?

    var body: some View {
        Label(
            health?.productionLabel ?? "Production · Unknown",
            systemImage: health?.productionOperational == true
                ? "checkmark.circle.fill" : "shippingbox.circle"
        )
        .font(.caption.weight(.semibold))
        .foregroundStyle(color)
    }

    private var color: Color {
        guard let production = health?.growth?.production else { return .secondary }
        if !production.configured || production.state == "disabled" {
            return .secondary
        }
        return production.isOperational ? .green : .orange
    }
}

private struct CodexExecutionCard: View {
    let presentation: CodexExecutionPresentation

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            Label(presentation.headline, systemImage: "sparkles")
                .font(.caption.weight(.semibold))
            if let detail = presentation.detail {
                Text(detail)
            }
            Text(presentation.progress)
            Text(presentation.concurrency)
            Text(presentation.budget)
            if let credentialSwitch = presentation.credentialSwitch {
                Text(credentialSwitch)
            }
        }
        .font(.caption2)
        .foregroundStyle(.secondary)
        .fixedSize(horizontal: false, vertical: true)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(presentation.accessibilityLabel)
        .accessibilityValue(presentation.accessibilityValue)
    }
}

private struct QueueLaneView: View {
    let title: String
    let queue: QueueHealth?
    let detail: String

    var body: some View {
        VStack(alignment: .leading, spacing: 5) {
            Text(title.uppercased()).font(.caption2.weight(.semibold)).foregroundStyle(.secondary)
            HStack {
                Text("\(queue?.succeeded ?? 0, format: .number) / \(queue?.total ?? 0, format: .number)")
                    .font(.system(.body, design: .monospaced).weight(.medium))
                Spacer()
                Text(activity).font(.caption).foregroundStyle(.secondary)
            }
            ProgressView(value: Double(queue?.succeeded ?? 0), total: Double(max(1, queue?.total ?? 1)))
                .accessibilityLabel("\(title) progress")
                .accessibilityValue("\(queue?.succeeded ?? 0) of \(queue?.total ?? 0) \(detail)")
        }
    }

    private var activity: String {
        [
            "\(queue?.active ?? 0) active",
            "\(queue?.retryWait ?? 0) retry",
            "\(queue?.quotaWait ?? 0) quota",
            "\(queue?.deadLetter ?? 0) dead",
        ].joined(separator: " · ")
    }
}

private struct CodexConcurrencyMenu: View {
    @ObservedObject var store: RigStore

    var body: some View {
        Menu {
            ForEach(RigStore.concurrencyChoices, id: \.self) { value in
                Button {
                    store.setConcurrency(RigStore.codexProvider, value)
                } label: {
                    if store.requestedConcurrency[RigStore.codexProvider] == nil,
                       store.configuredConcurrency(RigStore.codexProvider) == value
                    {
                        Label("\(value)", systemImage: "checkmark")
                    } else {
                        Text("\(value)")
                    }
                }
            }
        } label: {
            Text(codexLabel)
        }
        .help(RigStore.codexConcurrencyHelp)
        .accessibilityLabel("Codex translation concurrency")
        .accessibilityValue(codexLabel)
    }

    private var codexLabel: String {
        RigStore.concurrencyControlLabel(
            provider: "Codex",
            configured: store.configuredConcurrency(RigStore.codexProvider),
            requested: store.requestedConcurrency[RigStore.codexProvider],
            state: store.concurrencyApplicationState[RigStore.codexProvider]
        )
    }
}
