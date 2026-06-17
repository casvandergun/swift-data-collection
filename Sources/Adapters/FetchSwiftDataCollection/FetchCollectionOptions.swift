import Foundation
import SwiftData
import SwiftDataCollection

public typealias FetchCollectionHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (FetchCollectionContext<Model, ID>) async throws -> [CollectionRow]

public typealias FetchMutationHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (CollectionMutationContext<Model, ID>) async throws -> Void

public struct FetchCollectionContext<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: Sendable {
    public let debugName: String
    public let modelName: String
    public let scopeID: String
}

public enum FetchMissingRowPolicy: Sendable, Hashable {
    case deleteSyncedRows
    case keepLocalRows
}

public struct FetchCollectionOptions<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: Sendable {
    public let debugName: String
    public let identifier: CollectionModelIdentifier<Model, ID>
    public let modelName: String
    public let scopeID: String
    public let missingRowPolicy: FetchMissingRowPolicy
    public let fetch: FetchCollectionHandler<Model, ID>
    public let onApply: CollectionApplyHandler?
    public let onInsert: FetchMutationHandler<Model, ID>?
    public let onUpdate: FetchMutationHandler<Model, ID>?
    public let onDelete: FetchMutationHandler<Model, ID>?

    public init(
        debugName: String? = nil,
        scopeID: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        modelName: String = String(reflecting: Model.self),
        missingRowPolicy: FetchMissingRowPolicy = .deleteSyncedRows,
        fetch: @escaping FetchCollectionHandler<Model, ID>,
        onApply: CollectionApplyHandler? = nil,
        onInsert: FetchMutationHandler<Model, ID>? = nil,
        onUpdate: FetchMutationHandler<Model, ID>? = nil,
        onDelete: FetchMutationHandler<Model, ID>? = nil
    ) {
        self.debugName = debugName ?? modelName
        self.identifier = identifier
        self.modelName = modelName
        self.scopeID = scopeID
        self.missingRowPolicy = missingRowPolicy
        self.fetch = fetch
        self.onApply = onApply
        self.onInsert = onInsert
        self.onUpdate = onUpdate
        self.onDelete = onDelete
    }

    public func collectionOptions() -> CollectionOptions<Model, ID> {
        let adapter = CollectionAdapter<Model, ID>(
            sourceID: Self.sourceID(modelName: modelName, scopeID: scopeID),
            makeRuntime: { context in
                FetchCollectionAdapterRuntime(
                    configuration: self,
                    context: context
                )
            }
        )

        return CollectionOptions(
            debugName: debugName,
            identifier: identifier,
            modelName: modelName,
            adapter: adapter,
            onApply: onApply,
            onInsert: Self.wrap(onInsert),
            onUpdate: Self.wrap(onUpdate),
            onDelete: Self.wrap(onDelete)
        )
    }

    static func sourceID(modelName: String, scopeID: String) -> String {
        "fetch:\(modelName):\(scopeID)"
    }

    private static func wrap(
        _ handler: FetchMutationHandler<Model, ID>?
    ) -> CollectionAdapterMutationHandler<Model, ID>? {
        guard let handler else { return nil }

        return { context in
            try await handler(context)
            return .refresh
        }
    }
}

public func fetchCollectionOptions<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>(
    debugName: String? = nil,
    scopeID: String,
    identifier: CollectionModelIdentifier<Model, ID>,
    modelName: String = String(reflecting: Model.self),
    missingRowPolicy: FetchMissingRowPolicy = .deleteSyncedRows,
    fetch: @escaping FetchCollectionHandler<Model, ID>,
    onApply: CollectionApplyHandler? = nil,
    onInsert: FetchMutationHandler<Model, ID>? = nil,
    onUpdate: FetchMutationHandler<Model, ID>? = nil,
    onDelete: FetchMutationHandler<Model, ID>? = nil
) -> CollectionOptions<Model, ID> {
    FetchCollectionOptions(
        debugName: debugName,
        scopeID: scopeID,
        identifier: identifier,
        modelName: modelName,
        missingRowPolicy: missingRowPolicy,
        fetch: fetch,
        onApply: onApply,
        onInsert: onInsert,
        onUpdate: onUpdate,
        onDelete: onDelete
    )
    .collectionOptions()
}
