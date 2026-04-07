import ElectricSwift
import SwiftDataCollection

extension CollectionValue {
    init(electricValue: ElectricValue) {
        switch electricValue {
        case .string(let value):
            self = .string(value)
        case .integer(let value):
            self = .integer(value)
        case .double(let value):
            self = .double(value)
        case .boolean(let value):
            self = .boolean(value)
        case .object(let value):
            self = .object(value.mapValues(CollectionValue.init(electricValue:)))
        case .array(let value):
            self = .array(value.map(CollectionValue.init(electricValue:)))
        case .null:
            self = .null
        }
    }
}

extension CollectionRow {
    init(electricRow: ElectricRow) {
        self = electricRow.mapValues(CollectionValue.init(electricValue:))
    }
}

extension ElectricValue {
    init(collectionValue: CollectionValue) {
        switch collectionValue {
        case .string(let value):
            self = .string(value)
        case .integer(let value):
            self = .integer(value)
        case .double(let value):
            self = .double(value)
        case .boolean(let value):
            self = .boolean(value)
        case .object(let value):
            self = .object(value.mapValues(ElectricValue.init(collectionValue:)))
        case .array(let value):
            self = .array(value.map(ElectricValue.init(collectionValue:)))
        case .null:
            self = .null
        }
    }
}

extension ElectricRow {
    init(collectionRow: CollectionRow) {
        self = collectionRow.mapValues(ElectricValue.init(collectionValue:))
    }
}

extension CollectionDebugLogger {
    var electricDebugLogger: ElectricDebugLogger {
        ElectricDebugLogger { event in
            self.log(
                CollectionDebugLevel(electricDebugLevel: event.level),
                category: event.category,
                message: event.message,
                metadata: event.metadata
            )
        }
    }
}

extension CollectionDebugLevel {
    init(electricDebugLevel: ElectricDebugLevel) {
        switch electricDebugLevel {
        case .trace:
            self = .trace
        case .debug:
            self = .debug
        case .info:
            self = .info
        case .error:
            self = .error
        }
    }
}
