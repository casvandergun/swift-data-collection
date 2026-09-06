import Foundation
import SwiftData

/// Synchronously advances authoritative row evidence and derives the visible
/// SwiftData model from that evidence plus durable local intent.
///
/// Every method mutates only the supplied context. The caller owns the write
/// gate and the single atomic `save()` that also persists outbox/checkpoint work.
package struct CollectionMaterializer<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> {
    package let context: ModelContext
    package let collectionID: String
    package let modelName: String
    package let identifier: CollectionModelIdentifier<Model, ID>
    package let rowDecoder: CollectionRowDecoder

    package init(
        context: ModelContext,
        collectionID: String,
        modelName: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        rowDecoder: CollectionRowDecoder
    ) {
        self.context = context
        self.collectionID = collectionID
        self.modelName = modelName
        self.identifier = identifier
        self.rowDecoder = rowDecoder
    }

    /// Captures the baseline before the first optimistic mutation for a key.
    package func captureBaselineIfNeeded(
        for key: String,
        operation: CollectionMutationOperation
    ) throws {
        guard try fetchBase(for: key) == nil else { return }

        let existing = try fetchModel(for: key)
        let hasLegacyIntent = try orderedOverlays(for: key).isEmpty == false
        let evidence: CollectionBaselineEvidence

        if hasLegacyIntent {
            evidence = .unknown
        } else if operation == .create {
            // A staged create is local-only and promotion starts ordinary intent
            // from authoritative absence.
            evidence = .absent
        } else if let existing, existing.collectionSyncState == .synced {
            evidence = .observedRow(try existing.collectionRow())
        } else if existing == nil {
            evidence = .absent
        } else {
            // Migrated dirty/error rows cannot establish authoritative truth.
            evidence = .unknown
        }

        try insertBase(for: key, evidence: evidence)
    }

    /// Installs a recovery-required baseline for migrated dirty state.
    package func ensureUnknownBaseline(for key: String) throws {
        guard try fetchBase(for: key) == nil else { return }
        try insertBase(for: key, evidence: .unknown)
    }

    /// Applies normalized adapter evidence to the retained baseline.
    @discardableResult
    package func apply(
        _ evidence: CollectionAuthoritativeEvidence,
        for key: String
    ) throws -> Bool {
        let base: CollectionAuthoritativeBase
        if let existingBase = try fetchBase(for: key) {
            base = existingBase
        } else if case .patch = evidence,
                  try orderedOverlays(for: key).isEmpty,
                  try fetchModel(for: key) == nil {
            // A partial row for a clean, absent key is not enough to establish
            // either presence or absence and does not justify retained metadata.
            return false
        } else {
            base = try makeBaseForIncomingEvidence(key: key, evidence: evidence)
        }

        switch evidence {
        case .replacement(let row):
            try base.setEvidence(.observedRow(row))
            return true
        case .absence:
            try base.setEvidence(.absent)
            return true
        case .patch(let patch):
            switch try base.evidence() {
            case .observedRow(let row):
                try base.setEvidence(.observedRow(CollectionRowPatcher.applying(patch: patch, to: row)))
                return true
            case .acceptedRow(let row):
                // The complete representation still contains values that were
                // accepted but never observed, so retain accepted provenance.
                try base.setEvidence(.acceptedRow(CollectionRowPatcher.applying(patch: patch, to: row)))
                return true
            case .unknown, .absent:
                // An incomplete patch cannot establish a row.
                return false
            }
        }
    }

    /// Advances the baseline using a representation accepted by an immediate
    /// completion handler. Update values must contain changed fields only.
    package func accept(
        _ acceptance: CollectionImmediateAcceptance,
        for key: String
    ) throws {
        let base = try fetchBase(for: key) ?? makeBaseForImmediateAcceptance(key: key)

        switch acceptance {
        case .create(let row):
            try base.setEvidence(.acceptedRow(row))
        case .delete:
            try base.setEvidence(.absent)
        case .update(let changes):
            switch try base.evidence() {
            case .acceptedRow(let row), .observedRow(let row):
                try base.setEvidence(.acceptedRow(CollectionRowPatcher.applying(patch: changes, to: row)))
            case .absent:
                // Update is not implicitly an upsert.
                break
            case .unknown:
                // Dropping the overlay after immediate completion would lose
                // intent because there is no safe row on which to apply it.
                throw CollectionMaterializationError.unknownBaseline(key: key)
            }
        }
    }

    /// Invalidates evidence after an adapter reset. Local intent and the visible
    /// row survive, but destructive repair remains unavailable until refetch.
    package func invalidateBaseline(for key: String) throws {
        guard let base = try fetchBase(for: key) else { return }
        if try orderedOverlays(for: key).isEmpty {
            context.delete(base)
        } else {
            try base.setEvidence(.unknown)
        }
    }

    package func invalidateAllBaselines() throws {
        for base in try fetchBases() {
            if try orderedOverlays(for: base.targetKey).isEmpty {
                context.delete(base)
            } else {
                try base.setEvidence(.unknown)
            }
        }
    }

    package func baselineEvidence(for key: String) throws -> CollectionBaselineEvidence? {
        try fetchBase(for: key)?.evidence()
    }

    package func baselineKeys() throws -> Set<String> {
        Set(try fetchBases().map(\.targetKey))
    }

    package func materialize(keys: some Sequence<String>) throws {
        for key in Set(keys) {
            try materialize(key: key)
        }
    }

    /// Rebuilds one visible row from known base state and surviving intent.
    package func materialize(key: String) throws {
        guard let base = try fetchBase(for: key) else { return }
        let overlays = try orderedOverlays(for: key)

        if overlays.isEmpty,
           let existing = try fetchModel(for: key),
           existing.collectionSyncState == .stagedCreate {
            existing.collectionPendingMutationCount = 0
            context.delete(base)
            return
        }

        guard case let .known(initialRow) = LogicalRow(evidence: try base.evidence()) else {
            if overlays.isEmpty {
                // With no remaining local intent an unknown base has no valid
                // lifetime. Preserve the visible row for the adapter's reset
                // policy to handle, and remove only the hidden metadata.
                context.delete(base)
                return
            }
            // Legacy unknown state remains inspectable. Never reinterpret the
            // current optimistic model as authoritative or delete it.
            try refreshStateOnly(key: key, overlays: overlays)
            return
        }

        var logicalRow = initialRow
        var recoverableDeletedRow: CollectionRow?
        for overlay in overlays {
            switch overlay.mutation.operation {
            case .create:
                logicalRow = try overlay.payload()
                recoverableDeletedRow = nil
            case .update:
                // Decode the persisted intent even when authoritative absence
                // makes the patch inapplicable. Corrupt outbox data must fail
                // loudly; treating it as an empty patch would silently lose
                // the user's intent.
                let changes = try overlay.changedValues()
                guard let row = logicalRow else { continue }
                logicalRow = CollectionRowPatcher.applying(
                    patch: changes,
                    to: row
                )
            case .delete:
                if let logicalRow {
                    recoverableDeletedRow = logicalRow
                }
                logicalRow = nil
            }
        }

        if overlays.isEmpty {
            try replaceVisibleModel(for: key, with: logicalRow, overlays: [])
            context.delete(base)
            return
        }

        // Soft deletes remain visible and recoverable while their intent lives.
        let hasDelete = overlays.contains { $0.mutation.operation == .delete }
        let visibleRow = logicalRow ?? (hasDelete ? recoverableDeletedRow : nil)
        try replaceVisibleModel(for: key, with: visibleRow, overlays: overlays)
    }

    /// Rebuilds dispatch payloads only for attempts whose representation has
    /// never been frozen/submitted. Recorded changed-field intent is preserved.
    package func rebuildNeverSubmittedSuccessorPayloads(
        for keys: Set<String>
    ) throws {
        for key in keys {
            guard let base = try fetchBase(for: key) else { continue }
            let initial: CollectionRow?
            switch try base.evidence() {
            case .acceptedRow(let row), .observedRow(let row):
                initial = row
            case .absent:
                initial = nil
            case .unknown:
                throw CollectionMaterializationError.unknownBaseline(key: key)
            }

            var prefix = initial
            for overlay in try orderedOverlays(for: key) {
                let mutation = overlay.mutation
                switch mutation.operation {
                case .create:
                    prefix = try overlay.payload()
                case .update:
                    let changes = try overlay.changedValues()
                    guard let row = prefix else { continue }
                    let rebuilt = CollectionRowPatcher.applying(
                        patch: changes,
                        to: row
                    )
                    if overlay.hasFrozenOrSubmittedRepresentation == false {
                        do {
                            mutation.payloadData = try JSONEncoder().encode(rebuilt)
                        } catch {
                            throw CollectionMaterializationError.invalidPersistedRow(
                                key: mutation.targetKey
                            )
                        }
                    }
                    prefix = rebuilt
                case .delete:
                    prefix = nil
                }
            }
        }
    }

    private func makeBaseForIncomingEvidence(
        key: String,
        evidence: CollectionAuthoritativeEvidence
    ) throws -> CollectionAuthoritativeBase {
        let baseline: CollectionBaselineEvidence
        switch evidence {
        case .replacement(let row):
            baseline = .observedRow(row)
        case .absence:
            baseline = .absent
        case .patch:
            if try orderedOverlays(for: key).isEmpty == false {
                baseline = .unknown
            } else if let existing = try fetchModel(for: key),
                      existing.collectionSyncState == .synced {
                baseline = .observedRow(try existing.collectionRow())
            } else {
                baseline = .unknown
            }
        }
        return try insertBase(for: key, evidence: baseline)
    }

    private func makeBaseForImmediateAcceptance(key: String) throws -> CollectionAuthoritativeBase {
        if let existing = try fetchModel(for: key), existing.collectionSyncState == .synced {
            return try insertBase(for: key, evidence: .observedRow(try existing.collectionRow()))
        }
        return try insertBase(for: key, evidence: .unknown)
    }

    @discardableResult
    private func insertBase(
        for key: String,
        evidence: CollectionBaselineEvidence
    ) throws -> CollectionAuthoritativeBase {
        let base = try CollectionAuthoritativeBase(
            collectionID: collectionID,
            modelName: modelName,
            targetKey: key,
            evidence: evidence
        )
        context.insert(base)
        return base
    }

    private func fetchBase(for key: String) throws -> CollectionAuthoritativeBase? {
        let identity = CollectionAuthoritativeBase.identity(
            collectionID: collectionID,
            modelName: modelName,
            targetKey: key
        )
        guard let base = try context.fetch(FetchDescriptor<CollectionAuthoritativeBase>())
            .first(where: { $0.id == identity }) else {
            return nil
        }
        guard base.collectionID == collectionID,
              base.modelName == modelName,
              base.targetKey == key else {
            throw CollectionMaterializationError.invalidPersistedBaseline(key: key)
        }
        return base
    }

    private func fetchBases() throws -> [CollectionAuthoritativeBase] {
        try context.fetch(FetchDescriptor<CollectionAuthoritativeBase>()).filter {
            $0.collectionID == collectionID && $0.modelName == modelName
        }
    }

    private func fetchModel(for key: String) throws -> Model? {
        try context.fetch(identifier.fetchDescriptor(forSerializedKey: key)).first
    }

    private func orderedOverlays(for key: String) throws -> [OrderedOverlay] {
        let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            .filter { $0.collectionID == collectionID && $0.modelName == modelName }
        let transactionsByID = Dictionary(uniqueKeysWithValues: transactions.map { ($0.id, $0) })

        return try context.fetch(FetchDescriptor<PendingCollectionMutation>())
            .filter {
                $0.modelName == modelName &&
                $0.targetKey == key &&
                $0.status.participatesInReconciliationOverlay &&
                transactionsByID[$0.transactionID] != nil
            }
            .map { OrderedOverlay(mutation: $0, transaction: transactionsByID[$0.transactionID]!) }
            .sorted(by: OrderedOverlay.precedes)
    }

    private func replaceVisibleModel(
        for key: String,
        with row: CollectionRow?,
        overlays: [OrderedOverlay]
    ) throws {
        let existing = try fetchModel(for: key)
        guard let row else {
            if let existing {
                context.delete(existing)
            }
            return
        }

        let model: Model
        if let existing {
            // Decode a candidate before touching the existing managed object so
            // a malformed or mismatched persisted row cannot partially mutate
            // the visible SwiftData model.
            let candidate = try Model(collectionRow: row, decoder: rowDecoder)
            guard try identifier.get(candidate) == identifier.deserialize(key) else {
                throw CollectionMaterializationError.invalidPersistedRow(key: key)
            }
            try existing.apply(collectionRow: row, decoder: rowDecoder)
            model = existing
        } else {
            model = try Model(collectionRow: row, decoder: rowDecoder)
            guard try identifier.get(model) == identifier.deserialize(key) else {
                throw CollectionMaterializationError.invalidPersistedRow(key: key)
            }
            context.insert(model)
        }
        applyState(to: model, overlays: overlays)
    }

    private func refreshStateOnly(key: String, overlays: [OrderedOverlay]) throws {
        guard let existing = try fetchModel(for: key) else { return }
        applyState(to: existing, overlays: overlays)
    }

    private func applyState(to model: Model, overlays: [OrderedOverlay]) {
        model.collectionPendingMutationCount = overlays.count
        if overlays.isEmpty {
            if model.collectionSyncState != .stagedCreate {
                model.collectionSyncState = .synced
            }
        } else if overlays.contains(where: { $0.mutation.status == .conflicted }) {
            model.collectionSyncState = .conflicted
        } else if overlays.contains(where: { $0.mutation.status == .failed }) {
            model.collectionSyncState = .error
        } else if overlays.contains(where: { $0.mutation.operation == .delete }) {
            model.collectionSyncState = .pendingDelete
        } else if overlays.contains(where: { $0.mutation.operation == .create }) {
            model.collectionSyncState = .pendingCreate
        } else {
            model.collectionSyncState = .pendingUpdate
        }
    }
}

private extension CollectionMaterializer {
    struct OrderedOverlay {
        let mutation: PendingCollectionMutation
        let transaction: PendingCollectionTransaction

        var hasFrozenOrSubmittedRepresentation: Bool {
            transaction.submittedMutationsData != nil ||
            transaction.dispatchGroupID != nil ||
            transaction.attemptCount > 0 ||
            mutation.attemptCount > 0
        }

        func payload() throws -> CollectionRow {
            do {
                return try JSONDecoder().decode(CollectionRow.self, from: mutation.payloadData)
            } catch {
                throw CollectionMaterializationError.invalidPersistedRow(key: mutation.targetKey)
            }
        }

        func changedValues() throws -> CollectionRow {
            let changedFields: Set<String>
            if let data = mutation.changedFieldsData {
                do {
                    changedFields = try JSONDecoder().decode(Set<String>.self, from: data)
                } catch {
                    throw CollectionMaterializationError.invalidPersistedRow(key: mutation.targetKey)
                }
            } else {
                changedFields = []
            }
            return try payload().filter { changedFields.contains($0.key) }
        }

        static func precedes(_ lhs: Self, _ rhs: Self) -> Bool {
            if lhs.transaction.sequenceNumber != rhs.transaction.sequenceNumber {
                return lhs.transaction.sequenceNumber < rhs.transaction.sequenceNumber
            }
            if lhs.transaction.createdAt != rhs.transaction.createdAt {
                return lhs.transaction.createdAt < rhs.transaction.createdAt
            }
            let lhsTransactionID = lhs.transaction.id.uuidString
            let rhsTransactionID = rhs.transaction.id.uuidString
            if lhsTransactionID != rhsTransactionID {
                return lhsTransactionID < rhsTransactionID
            }
            if lhs.mutation.ordinal != rhs.mutation.ordinal {
                return lhs.mutation.ordinal < rhs.mutation.ordinal
            }
            if lhs.mutation.createdAt != rhs.mutation.createdAt {
                return lhs.mutation.createdAt < rhs.mutation.createdAt
            }
            return lhs.mutation.id.uuidString < rhs.mutation.id.uuidString
        }
    }

    enum LogicalRow {
        case unknown
        case known(CollectionRow?)

        init(evidence: CollectionBaselineEvidence) {
            switch evidence {
            case .unknown:
                self = .unknown
            case .absent:
                self = .known(nil)
            case .acceptedRow(let row), .observedRow(let row):
                self = .known(row)
            }
        }
    }
}
