import Foundation

private enum OriginHealthCodingKey: String, CodingKey {
    case active = "active"
    case consecutiveFailures = "consecutiveFailures"
    case cooldownUntil = "cooldownUntil"
    case lastCompletedAt = "lastCompletedAt"
    case nextAllowedAt = "nextAllowedAt"
    case origin = "origin"
    case state = "state"
    case stopReason = "stopReason"
}

internal struct OriginHealth: Codable, Hashable, Identifiable {
    private static let validStates = Set([
        "blocked",
        "challenge_wait",
        "disabled",
        "disk_wait",
        "healthy",
        "network_wait",
        "rate_wait",
    ])

    let active: Bool
    let consecutiveFailures: Int
    let cooldownUntil: Int
    let lastCompletedAt: Int?
    let nextAllowedAt: Int
    let origin: String
    let state: String?
    let stopReason: String?

    var id: String {
        origin
    }

    var isValid: Bool {
        consecutiveFailures >= 0 && cooldownUntil >= 0
            && nextAllowedAt >= 0 && lastCompletedAt.map { timestamp in timestamp >= 0 } != false
            && URL(string: origin)?.host != nil
            && state.map { value in Self.validStates.contains(value) } != false
            && stopReason.map { reason in
                reason.range(of: #"^[A-Z][A-Z0-9_]{1,127}$"#, options: .regularExpression) != nil
            } != false
    }

    init(
        active: Bool,
        consecutiveFailures: Int,
        cooldownUntil: Int,
        lastCompletedAt: Int?,
        nextAllowedAt: Int,
        origin: String,
        stopReason: String?,
        state: String? = nil
    ) {
        self.active = active
        self.consecutiveFailures = consecutiveFailures
        self.cooldownUntil = cooldownUntil
        self.lastCompletedAt = lastCompletedAt
        self.nextAllowedAt = nextAllowedAt
        self.origin = origin
        self.state = state
        self.stopReason = stopReason
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: OriginHealthCodingKey.self)
        active = try values.decode(Bool.self, forKey: .active)
        consecutiveFailures = try values.decode(Int.self, forKey: .consecutiveFailures)
        cooldownUntil = try values.decode(Int.self, forKey: .cooldownUntil)
        lastCompletedAt = try values.decodeIfPresent(Int.self, forKey: .lastCompletedAt)
        nextAllowedAt = try values.decode(Int.self, forKey: .nextAllowedAt)
        origin = try values.decode(String.self, forKey: .origin)
        state = try values.decodeIfPresent(String.self, forKey: .state)
        stopReason = try values.decodeIfPresent(String.self, forKey: .stopReason)
    }
}
