import NeonRealtimeSwift
import SwiftData
import SwiftDataCollection

public enum NeonCollectionMutationSubmission: Sendable, Hashable {
    /// The decimal PostgreSQL transaction ID from a committed application write.
    case committed(txid: String)
    /// The server accepted the request without changing any rows.
    case unchanged
}

public typealias NeonCollectionMutationHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (CollectionMutationContext<Model, ID>) async throws -> NeonCollectionMutationSubmission

public enum NeonCollectionError: Error, Sendable, Equatable {
    /// The HTTP status of a failed seal request. The collection retries 408, 429, and 5xx.
    case sealHTTPStatus(Int)
    case queryFingerprintChanged
    case missingStableIdentifier
    case duplicateModelKey(String)
    case unknownRowID(String)
    case missingSnapshot
    case invalidTransactionID
}

/// Syncs one sealed query into the models selected by 'ownedRows'.
/// Keep 'scopeID' stable for the query's database and ownership scope. The loader
/// must return a fresh capability for the same query on each call. Throw
/// `NeonRealtimeSubscriptionError.sealHTTPStatus` for unsuccessful HTTP responses.
public func neonCollectionOptions<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>(
    debugName: String? = nil,
    scopeID: String,
    identifier: CollectionModelIdentifier<Model, ID>,
    client: RealtimeClient,
    loadQuery: @escaping @Sendable () async throws -> SealedLiveQuery,
    ownedRows: @escaping @Sendable () -> FetchDescriptor<Model>,
    decodeRow: @escaping @Sendable (NeonRow, [NeonColumn]) throws -> CollectionRow,
    onInsert: NeonCollectionMutationHandler<Model, ID>? = nil,
    onUpdate: NeonCollectionMutationHandler<Model, ID>? = nil,
    onDelete: NeonCollectionMutationHandler<Model, ID>? = nil
) -> CollectionOptions<Model, ID> {
    let modelName = String(reflecting: Model.self)
    return CollectionOptions(
        debugName: debugName ?? modelName,
        identifier: identifier,
        modelName: modelName,
        adapter: CollectionAdapter(sourceID: "neon:\(scopeID)") { context in
            let subscription = client.subscription {
                do {
                    return try await loadQuery()
                } catch NeonCollectionError.sealHTTPStatus(let status) {
                    throw NeonRealtimeSubscriptionError.sealHTTPStatus(status)
                }
            }
            return NeonCollectionRuntime(
                context: context,
                modelName: modelName,
                subscription: subscription,
                ownedRows: ownedRows,
                decodeRow: decodeRow
            )
        },
        onInsert: wrapMutation(onInsert),
        onUpdate: wrapMutation(onUpdate),
        onDelete: wrapMutation(onDelete)
    )
}

private func wrapMutation<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
    _ handler: NeonCollectionMutationHandler<Model, ID>?
) -> CollectionAdapterMutationHandler<Model, ID>? {
    guard let handler else { return nil }
    return { context in
        switch try await handler(context) {
        case .committed(let txid):
            do {
                _ = try parseNeonTransactionID(txid)
            } catch {
                throw CollectionNonRetriableError(
                    "The Neon write returned an invalid committed transaction ID.",
                    disposition: .quarantine
                )
            }
            return .awaitTokens([txid])
        case .unchanged:
            return .unchanged
        }
    }
}

func parseNeonTransactionID(_ value: String) throws -> UInt64 {
    guard let id = UInt64(value), id > 0, String(id) == value else {
        throw NeonCollectionError.invalidTransactionID
    }
    return id
}
