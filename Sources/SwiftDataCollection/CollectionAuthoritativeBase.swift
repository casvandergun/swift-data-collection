import Foundation
import SwiftData

/// The authoritative evidence retained while a row has unresolved local intent.
///
/// This is a diagnostic snapshot, not a second query surface. `acceptedRow` is a
/// representation accepted by an immediate-completion handler; `observedRow` was
/// supplied by an adapter as authoritative server evidence.
public enum CollectionBaselineEvidence: Sendable, Hashable, Codable {
    case unknown
    case absent
    case acceptedRow(CollectionRow)
    case observedRow(CollectionRow)
}

package enum CollectionAuthoritativeEvidence: Sendable, Hashable {
    case replacement(CollectionRow)
    case patch(CollectionRow)
    case absence
}

package enum CollectionImmediateAcceptance: Sendable, Hashable {
    case create(CollectionRow)
    case update(CollectionRow)
    case delete
}

public enum CollectionMaterializationError: Error, Sendable, Hashable {
    case invalidPersistedBaseline(key: String)
    case invalidPersistedRow(key: String)
    case unknownBaseline(key: String)
}

@Model
package final class CollectionAuthoritativeBase {
    @Attribute(.unique) package var id: String = ""
    package var collectionID: String = ""
    package var modelName: String = ""
    package var targetKey: String = ""
    package var stateRawValue: String = "unknown"
    package var provenanceRawValue: String? = nil
    package var rowData: Data? = nil

    package init(
        collectionID: String,
        modelName: String,
        targetKey: String,
        evidence: CollectionBaselineEvidence
    ) throws {
        self.id = Self.identity(
            collectionID: collectionID,
            modelName: modelName,
            targetKey: targetKey
        )
        self.collectionID = collectionID
        self.modelName = modelName
        self.targetKey = targetKey
        try setEvidence(evidence)
    }

    package func evidence() throws -> CollectionBaselineEvidence {
        guard let state = State(rawValue: stateRawValue) else {
            throw CollectionMaterializationError.invalidPersistedBaseline(key: targetKey)
        }

        switch state {
        case .unknown:
            return .unknown
        case .absent:
            return .absent
        case .row:
            guard let rowData,
                  let row = try? JSONDecoder().decode(CollectionRow.self, from: rowData),
                  let provenanceRawValue,
                  let provenance = Provenance(rawValue: provenanceRawValue) else {
                throw CollectionMaterializationError.invalidPersistedRow(key: targetKey)
            }
            switch provenance {
            case .accepted:
                return .acceptedRow(row)
            case .observed:
                return .observedRow(row)
            }
        }
    }

    package func setEvidence(_ evidence: CollectionBaselineEvidence) throws {
        switch evidence {
        case .unknown:
            stateRawValue = State.unknown.rawValue
            provenanceRawValue = nil
            rowData = nil
        case .absent:
            stateRawValue = State.absent.rawValue
            provenanceRawValue = nil
            rowData = nil
        case .acceptedRow(let row):
            let encoded = try JSONEncoder().encode(row)
            stateRawValue = State.row.rawValue
            provenanceRawValue = Provenance.accepted.rawValue
            rowData = encoded
        case .observedRow(let row):
            let encoded = try JSONEncoder().encode(row)
            stateRawValue = State.row.rawValue
            provenanceRawValue = Provenance.observed.rawValue
            rowData = encoded
        }
    }

    package static func identity(
        collectionID: String,
        modelName: String,
        targetKey: String
    ) -> String {
        // A JSON tuple is unambiguous even when any component contains separators.
        // Encoding a String array has a deterministic element order.
        let components = [collectionID, modelName, targetKey]
        guard let data = try? JSONEncoder().encode(components),
              let result = String(data: data, encoding: .utf8) else {
            // All Swift Strings are JSON encodable, so this is unreachable while
            // still keeping identity construction non-throwing for predicates.
            return "[]"
        }
        return result
    }

    private enum State: String {
        case unknown
        case absent
        case row
    }

    private enum Provenance: String {
        case accepted
        case observed
    }
}
