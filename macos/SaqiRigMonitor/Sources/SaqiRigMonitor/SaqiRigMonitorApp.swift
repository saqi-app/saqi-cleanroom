import SwiftUI

@main
internal struct SaqiRigMonitorApp: App {
    @StateObject private var store = RigStore()

    var body: some Scene {
        MenuBarExtra {
            MonitorView(store: store)
        } label: {
            let presentation = MenuBarPresentation(status: store.operationalStatus)
            if presentation.requiresAttention {
                Image(systemName: presentation.symbol)
                    .renderingMode(.original)
                    .foregroundStyle(.red)
                    .accessibilityLabel(presentation.accessibilityLabel)
                    .help(presentation.accessibilityLabel)
            } else {
                Label("Saqi", systemImage: presentation.symbol)
                    .accessibilityLabel(presentation.accessibilityLabel)
            }
        }
        .menuBarExtraStyle(.window)
    }
}

internal struct MenuBarPresentation {
    let requiresAttention: Bool
    let accessibilityLabel: String

    var symbol: String {
        requiresAttention ? "exclamationmark.circle.fill" : "books.vertical.fill"
    }

    init(status: MonitorOperationalStatus) {
        requiresAttention = status.requiresAttention
        accessibilityLabel = "Saqi · \(status.label)"
    }
}
