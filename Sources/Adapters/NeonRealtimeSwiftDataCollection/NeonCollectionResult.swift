import NeonRealtimeSwift
import SwiftDataCollection

/// Tracks the current result's identity mapping. Row values live in SwiftData.
struct NeonCollectionResult {
    let columns: [NeonColumn]
    private var keysByRowID: [String: String] = [:]
    private var rowIDsByKey: [String: String] = [:]

    init(columns: [NeonColumn]) {
        self.columns = columns
    }

    mutating func upsert(
        _ rowID: String,
        key: String,
        row: CollectionRow,
        changes: inout [String: CollectionAuthoritativeEvidence]
    ) throws {
        if let owner = rowIDsByKey[key], owner != rowID {
            throw NeonCollectionError.duplicateModelKey(key)
        }
        if let formerKey = keysByRowID[rowID], formerKey != key {
            rowIDsByKey.removeValue(forKey: formerKey)
            changes[formerKey] = .absence
        }
        keysByRowID[rowID] = key
        rowIDsByKey[key] = rowID
        changes[key] = .replacement(row)
    }

    mutating func apply(
        _ batch: NeonChangeBatch,
        changes: inout [String: CollectionAuthoritativeEvidence],
        decode: (NeonRow, [NeonColumn]) throws -> (key: String, row: CollectionRow)
    ) throws {
        for change in batch.changes {
            switch change {
            case .upsert(let raw):
                let (key, row) = try decode(raw, columns)
                try upsert(raw.rowID, key: key, row: row, changes: &changes)
            case .remove(let rowID):
                guard let key = keysByRowID.removeValue(forKey: rowID) else {
                    throw NeonCollectionError.unknownRowID(rowID)
                }
                rowIDsByKey.removeValue(forKey: key)
                changes[key] = .absence
            case .move(let rowID, let predecessor):
                // SwiftData views own ordering. Both IDs still belong to the result.
                try requireRow(rowID)
                if let predecessor { try requireRow(predecessor) }
            }
        }
    }

    private func requireRow(_ rowID: String) throws {
        guard keysByRowID[rowID] != nil else {
            throw NeonCollectionError.unknownRowID(rowID)
        }
    }
}

/// Retains exact IDs until visibility covers them. Visibility can arrive with a
/// lower upper bound, so merge coverage instead of replacing the older proof.
struct NeonCollectionObservation: Sendable, Equatable {
    private var exact: Set<UInt64> = []
    private var upperBound: UInt64 = 0
    private var inProgress: Set<UInt64> = []

    func contains(_ token: String) -> Bool {
        guard let id = try? parseNeonTransactionID(token) else { return false }
        return exact.contains(id) || isVisible(id)
    }

    mutating func include(_ transactionIDs: [String]) throws {
        for token in transactionIDs {
            let id = try parseNeonTransactionID(token)
            if !isVisible(id) { exact.insert(id) }
        }
    }

    mutating func include(_ visibility: NeonTransactionVisibility) {
        if visibility.xmax >= upperBound {
            inProgress = visibility.inProgress.filter { !isVisible($0) }
            upperBound = visibility.xmax
        } else {
            inProgress = inProgress.filter { !visibility.includes(committedTransactionID: $0) }
        }
        exact = exact.filter { !isVisible($0) }
    }

    private func isVisible(_ id: UInt64) -> Bool {
        id < upperBound && !inProgress.contains(id)
    }
}
