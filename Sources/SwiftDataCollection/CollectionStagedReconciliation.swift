import SwiftData

package enum CollectionStagedUpsertMode: Sendable, Hashable {
    case replacement
    case patch
}

package enum CollectionStagedReconciliationOutcome: Sendable, Hashable {
    case notStaged
    case resolved
    case preservedDelete
}

package enum CollectionStagedReconciler {
    package static func applyUpsert<
        Model: SwiftDataCollectionModel,
        ID: Hashable & Sendable
    >(
        key: String,
        row: CollectionRow,
        mode: CollectionStagedUpsertMode,
        identifier: CollectionModelIdentifier<Model, ID>,
        rowDecoder: CollectionRowDecoder,
        in context: ModelContext
    ) throws -> CollectionStagedReconciliationOutcome {
        guard let existing = try context.fetch(
            identifier.fetchDescriptor(forSerializedKey: key)
        ).first,
        existing.collectionSyncState == .stagedCreate else {
            return .notStaged
        }

        let appliedRow = switch mode {
        case .replacement:
            row
        case .patch:
            CollectionRowPatcher.applying(
                patch: row,
                to: try existing.collectionRow()
            )
        }
        try existing.apply(collectionRow: appliedRow, decoder: rowDecoder)
        existing.collectionPendingMutationCount = 0
        existing.collectionSyncState = .synced
        return .resolved
    }

    package static func preserveDelete<
        Model: SwiftDataCollectionModel,
        ID: Hashable & Sendable
    >(
        key: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        in context: ModelContext
    ) throws -> CollectionStagedReconciliationOutcome {
        guard let existing = try context.fetch(
            identifier.fetchDescriptor(forSerializedKey: key)
        ).first,
        existing.collectionSyncState == .stagedCreate else {
            return .notStaged
        }
        return .preservedDelete
    }
}
