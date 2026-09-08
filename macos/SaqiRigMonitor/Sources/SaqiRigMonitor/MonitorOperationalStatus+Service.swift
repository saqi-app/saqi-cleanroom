extension MonitorOperationalStatus {
    static func immediateStatus(service: ServiceSnapshot?) -> Self? {
        if service?.onlySourceVerificationUnavailable == true {
            return Self(label: "Source identity unverified", severity: .attention, wait: nil)
        }
        return switch service?.actualState {
        case "fenced":
            Self(label: "Fenced · attention needed", severity: .attention, wait: nil)
        case "running_outdated":
            Self(label: "Running · restart needed", severity: .attention, wait: nil)
        case "starting":
            Self(label: "Starting · preparing lanes", severity: .waiting, wait: nil)
        case "stopped":
            Self(label: "Stopped", severity: .neutral, wait: nil)
        default:
            nil
        }
    }
}
