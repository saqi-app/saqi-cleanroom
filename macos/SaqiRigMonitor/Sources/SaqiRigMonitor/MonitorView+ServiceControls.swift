import SwiftUI

extension MonitorView {
    var serviceControls: some View {
        VStack(alignment: .leading) {
            if store.service?.sourceIdentityStatus == .retainedUnverified {
                Text("Source identity unverified · retained local identity")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
            HStack {
                if store.service?.allows("start") == true {
                    Button("Start", action: store.start)
                }
                if store.service?.allows("stop") == true {
                    Button("Stop", action: store.stop)
                }
                if store.service?.allows("restart") == true {
                    Button("Restart", action: store.restart)
                }
            }
            .disabled(!store.serviceControlsAvailable)
        }
    }
}
