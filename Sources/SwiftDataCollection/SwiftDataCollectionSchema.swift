import SwiftData

/// Composes application models with the collection runtime's persistence models.
/// Keep the returned types in each application schema version that uses this runtime.
public enum SwiftDataCollectionSchema {
    public static var models: [any PersistentModel.Type] {
        [PendingCollectionMutation.self, PendingCollectionTransaction.self,
         CollectionMetadata.self, CollectionAuthoritativeBase.self]
    }

    public static func models(
        including applicationModels: [any PersistentModel.Type]
    ) -> [any PersistentModel.Type] {
        var seen = Set<ObjectIdentifier>()
        return (applicationModels + models).filter {
            seen.insert(ObjectIdentifier($0)).inserted
        }
    }

    package static func validate(_ container: ModelContainer) throws {
        let registered = Set(container.schema.entities.map(\.name))
        let missing = models.map { String(describing: $0) }.filter { !registered.contains($0) }
        guard missing.isEmpty else {
            throw CollectionSchemaError.missingRuntimeModels(missing)
        }
    }
}

public enum CollectionSchemaError: Error, Sendable, Equatable {
    case missingRuntimeModels([String])
}
