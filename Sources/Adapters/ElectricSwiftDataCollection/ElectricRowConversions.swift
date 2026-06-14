import ElectricSwift
import Foundation
import SwiftDataCollection

extension CollectionValue {
    init(
        electricValue: ElectricValue,
        column: ElectricColumnDefinition? = nil,
        fieldType: CollectionFieldType? = nil
    ) {
        if let fieldType {
            self = CollectionValue.fieldTypeValue(from: electricValue, fieldType: fieldType)
            return
        }
        if let column, column.isCollectionDateColumn {
            self = CollectionValue.dateValue(from: electricValue, column: column)
            return
        }
        if let column, column.isCollectionUUIDColumn {
            self = CollectionValue.uuidValue(from: electricValue, column: column)
            return
        }

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
            self = .object(value.mapValues { CollectionValue(electricValue: $0) })
        case .array(let value):
            self = .array(value.map { CollectionValue(electricValue: $0) })
        case .null:
            self = .null
        }
    }

    private static func fieldTypeValue(from electricValue: ElectricValue, fieldType: CollectionFieldType) -> CollectionValue {
        switch fieldType {
        case .string:
            return matchingStringValue(from: electricValue, fieldType: fieldType)
        case .integer:
            return matchingIntegerValue(from: electricValue, fieldType: fieldType)
        case .double:
            return matchingDoubleValue(from: electricValue, fieldType: fieldType)
        case .boolean:
            return matchingBooleanValue(from: electricValue, fieldType: fieldType)
        case .date:
            return dateValue(from: electricValue)
        case .uuid:
            return uuidValue(from: electricValue)
        }
    }

    private static func matchingStringValue(from electricValue: ElectricValue, fieldType: CollectionFieldType) -> CollectionValue {
        switch electricValue {
        case .string(let value):
            return .string(value)
        case .array(let values):
            return .array(values.map { fieldTypeValue(from: $0, fieldType: fieldType) })
        case .null:
            return .null
        case .integer, .double, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func matchingIntegerValue(from electricValue: ElectricValue, fieldType: CollectionFieldType) -> CollectionValue {
        switch electricValue {
        case .integer(let value):
            return .integer(value)
        case .array(let values):
            return .array(values.map { fieldTypeValue(from: $0, fieldType: fieldType) })
        case .null:
            return .null
        case .string, .double, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func matchingDoubleValue(from electricValue: ElectricValue, fieldType: CollectionFieldType) -> CollectionValue {
        switch electricValue {
        case .double(let value):
            return .double(value)
        case .array(let values):
            return .array(values.map { fieldTypeValue(from: $0, fieldType: fieldType) })
        case .null:
            return .null
        case .string, .integer, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func matchingBooleanValue(from electricValue: ElectricValue, fieldType: CollectionFieldType) -> CollectionValue {
        switch electricValue {
        case .boolean(let value):
            return .boolean(value)
        case .array(let values):
            return .array(values.map { fieldTypeValue(from: $0, fieldType: fieldType) })
        case .null:
            return .null
        case .string, .integer, .double, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func dateValue(from electricValue: ElectricValue, column: ElectricColumnDefinition) -> CollectionValue {
        switch electricValue {
        case .string(let value):
            return ElectricDateParser.parse(value, postgresType: column.type).map(CollectionValue.date) ?? .string(value)
        case .array(let values):
            return .array(values.map { dateValue(from: $0, column: column.scalarColumn) })
        case .null:
            return .null
        case .integer, .double, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func dateValue(from electricValue: ElectricValue) -> CollectionValue {
        switch electricValue {
        case .string(let value):
            return ElectricDateParser.parseAny(value).map(CollectionValue.date) ?? .string(value)
        case .array(let values):
            return .array(values.map { dateValue(from: $0) })
        case .null:
            return .null
        case .integer, .double, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func uuidValue(from electricValue: ElectricValue, column: ElectricColumnDefinition) -> CollectionValue {
        switch electricValue {
        case .string(let value):
            return UUID(uuidString: value).map(CollectionValue.uuid) ?? .string(value)
        case .array(let values):
            return .array(values.map { uuidValue(from: $0, column: column.scalarColumn) })
        case .null:
            return .null
        case .integer, .double, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }

    private static func uuidValue(from electricValue: ElectricValue) -> CollectionValue {
        switch electricValue {
        case .string(let value):
            return UUID(uuidString: value).map(CollectionValue.uuid) ?? .string(value)
        case .array(let values):
            return .array(values.map { uuidValue(from: $0) })
        case .null:
            return .null
        case .integer, .double, .boolean, .object:
            return CollectionValue(electricValue: electricValue)
        }
    }
}

extension CollectionRow {
    init(electricRow: ElectricRow) {
        self = electricRow.mapValues { CollectionValue(electricValue: $0) }
    }

    init(
        electricRow: ElectricRow,
        schema: ElectricSchema,
        collectionSchema: CollectionSchema = .init()
    ) {
        self = Dictionary(uniqueKeysWithValues: electricRow.map { key, value in
            (
                key,
                CollectionValue(
                    electricValue: value,
                    column: schema[key],
                    fieldType: collectionSchema.fields[key]
                )
            )
        })
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
        case .date(let value):
            self = .string(ElectricDateFormatter.string(from: value))
        case .uuid(let value):
            self = .string(value.uuidString)
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

private extension ElectricColumnDefinition {
    var isCollectionDateColumn: Bool {
        switch type {
        case "timestamptz", "timestamp", "date":
            true
        default:
            false
        }
    }

    var isCollectionUUIDColumn: Bool {
        type == "uuid"
    }

    var scalarColumn: ElectricColumnDefinition {
        ElectricColumnDefinition(
            type: type,
            dims: nil,
            notNull: notNull,
            maxLength: maxLength,
            length: length,
            precision: precision,
            scale: scale,
            fields: fields
        )
    }
}

private enum ElectricDateParser {
    static func parseAny(_ value: String) -> Date? {
        parse(value, postgresType: "date")
            ?? parse(value, postgresType: "timestamp")
            ?? parse(value, postgresType: "timestamptz")
    }

    static func parse(_ value: String, postgresType: String) -> Date? {
        switch postgresType {
        case "date":
            return parseDate(value)
        case "timestamp":
            return parseTimestamp(value, hasExplicitTimeZone: false)
        case "timestamptz":
            return parseTimestamp(value, hasExplicitTimeZone: true)
        default:
            return nil
        }
    }

    private static func parseDate(_ value: String) -> Date? {
        makeFormatter("yyyy-MM-dd").date(from: value)
    }

    private static func parseTimestamp(_ value: String, hasExplicitTimeZone: Bool) -> Date? {
        let normalized = normalizeTimestamp(value, hasExplicitTimeZone: hasExplicitTimeZone)
        for format in timestampFormats {
            if let date = makeFormatter(format).date(from: normalized) {
                return date
            }
        }
        return nil
    }

    private static func normalizeTimestamp(_ value: String, hasExplicitTimeZone: Bool) -> String {
        var normalized = value.replacingOccurrences(of: " ", with: "T")
        if hasExplicitTimeZone {
            normalized = normalizeOffset(in: normalized)
        } else if normalized.last?.isNumber == true {
            normalized += "Z"
        }
        return normalized
    }

    private static func normalizeOffset(in value: String) -> String {
        if value.hasSuffix("+00") {
            return String(value.dropLast(3)) + "+00:00"
        }
        if value.hasSuffix("-00") {
            return String(value.dropLast(3)) + "-00:00"
        }

        guard value.count >= 5 else { return value }
        let suffix = value.suffix(5)
        guard (suffix.first == "+" || suffix.first == "-"),
              suffix.dropFirst().allSatisfy(\.isNumber) else {
            return value
        }

        let sign = suffix.first.map(String.init) ?? "+"
        let digits = suffix.dropFirst()
        return String(value.dropLast(5)) + "\(sign)\(digits.prefix(2)):\(digits.suffix(2))"
    }

    private static let timestampFormats = [
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXXXX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXXXX",
        "yyyy-MM-dd'T'HH:mm:ssXXXXX",
    ]

    private static func makeFormatter(_ format: String) -> DateFormatter {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = format
        return formatter
    }
}

private enum ElectricDateFormatter {
    static func string(from date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter.string(from: date)
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
