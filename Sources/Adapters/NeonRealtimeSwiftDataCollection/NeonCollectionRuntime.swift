import Foundation
import NeonRealtimeSwift
import SwiftData
import SwiftDataCollection

actor NeonCollectionRuntime<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: CollectionAdapterRuntime {
    private let context: CollectionAdapterContext<Model, ID>
    private let modelName: String
    private let subscription: RawLiveQuerySubscription
    private let ownedRows: @Sendable () -> FetchDescriptor<Model>
    private let decodeRow: @Sendable (NeonRow, [NeonColumn]) throws -> CollectionRow

    private var reader: Task<Void, Never>?
    private var pendingRefreshID: UUID?
    private var observation = NeonCollectionObservation()

    init(
        context: CollectionAdapterContext<Model, ID>,
        modelName: String,
        subscription: RawLiveQuerySubscription,
        ownedRows: @escaping @Sendable () -> FetchDescriptor<Model>,
        decodeRow: @escaping @Sendable (NeonRow, [NeonColumn]) throws -> CollectionRow
    ) {
        self.context = context
        self.modelName = modelName
        self.subscription = subscription
        self.ownedRows = ownedRows
        self.decodeRow = decodeRow
    }

    func start() {
        guard reader == nil else { return }
        reader = Task { [weak self] in
            await self?.read()
        }
    }

    func stop() async {
        pendingRefreshID = nil
        await stopReader()
    }

    func refresh() async {
        let id = UUID()
        pendingRefreshID = id
        await stopReader()
        guard pendingRefreshID == id else { return }
        pendingRefreshID = nil
        start()
    }

    private func stopReader() async {
        guard let reader else { return }
        reader.cancel()
        // The reader owns subscription teardown. A later lifecycle call may
        // start a replacement once this reader has finished.
        await reader.value
    }

    private func read() async {
        defer { reader = nil }
        var result: NeonCollectionResult?
        do {
            try Task.checkCancellation()
            for try await update in await subscription.updates() {
                try Task.checkCancellation()
                switch update {
                case .event(let event):
                    result = try await apply(event, to: result)
                case .stateChanged(.reconnecting):
                    result = nil
                case .stateChanged:
                    break
                }
            }
        } catch NeonRealtimeSubscriptionError.queryFingerprintChanged where !Task.isCancelled {
            await context.reportError(NeonCollectionError.queryFingerprintChanged)
        } catch where !Task.isCancelled {
            await context.reportError(error)
        } catch {}
        await subscription.stop()
    }

    private func apply(
        _ event: NeonRealtimeEvent,
        to result: NeonCollectionResult?
    ) async throws -> NeonCollectionResult? {
        var candidate = observation
        var changes: [String: CollectionAuthoritativeEvidence] = [:]
        switch event {
        case .snapshot(let snapshot):
            var result = NeonCollectionResult(columns: snapshot.columns)
            for raw in snapshot.rows {
                let (key, row) = try decode(raw, snapshot.columns)
                try result.upsert(raw.rowID, key: key, row: row, changes: &changes)
            }
            candidate.include(snapshot.visibility)
            for batch in snapshot.bufferedBatches {
                try result.apply(batch, changes: &changes, decode: decode)
                try candidate.include(batch.transactionIDs)
            }
            try await save(changes, observation: candidate, replacing: true)
            return result
        case .changes(let batch):
            guard var result else { throw NeonCollectionError.missingSnapshot }
            try result.apply(batch, changes: &changes, decode: decode)
            try candidate.include(batch.transactionIDs)
            try await save(changes, observation: candidate)
            return result
        case .progress(let visibility):
            guard result != nil else { throw NeonCollectionError.missingSnapshot }
            candidate.include(visibility)
            if candidate != observation {
                try await save([:], observation: candidate)
            }
            return result
        case .resetRequired:
            return nil
        }
    }

    private func decode(_ raw: NeonRow, _ columns: [NeonColumn]) throws -> (key: String, row: CollectionRow) {
        let row = try decodeRow(raw, columns)
        let model = try Model(collectionRow: row, decoder: context.rowDecoder)
        let key = context.identifier.key(for: model)
        guard !key.isEmpty else { throw NeonCollectionError.missingStableIdentifier }
        return (key, row)
    }

    private func save(
        _ incoming: [String: CollectionAuthoritativeEvidence],
        observation candidate: NeonCollectionObservation,
        replacing: Bool = false
    ) async throws {
        let context = context
        let modelName = modelName
        let ownedRows = ownedRows
        try await context.applyAndConfirm(candidate.contains) { modelContext in
            let materializer = CollectionMaterializer(
                context: modelContext,
                collectionID: context.collectionID,
                modelName: modelName,
                identifier: context.identifier,
                rowDecoder: context.rowDecoder
            )
            var changes = incoming
            if replacing {
                let keys = try Set(modelContext.fetch(ownedRows()).map(context.identifier.key(for:)))
                    .union(materializer.baselineKeys())
                for key in keys where changes[key] == nil {
                    changes[key] = .absence
                }
            }
            for (key, evidence) in changes {
                if case .replacement(let row) = evidence,
                   try CollectionStagedReconciler.applyUpsert(
                       key: key,
                       row: row,
                       mode: .replacement,
                       identifier: context.identifier,
                       rowDecoder: context.rowDecoder,
                       in: modelContext
                   ) == .resolved {
                    continue
                }
                try materializer.apply(evidence, for: key)
                try materializer.materialize(key: key)
            }
        }
        observation = candidate
    }
}
