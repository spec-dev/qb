import {
    QueryPayload,
    StringKeyMap,
    Filters,
    FilterOp,
    SelectOptions,
    OrderByDirection,
} from './types'
import { ident, literal } from './pgFormat'
import humps from './humps'

const filterOpValues = new Set(Object.values(FilterOp))

const identPath = (value: string): string =>
    value
        .split('.')
        .map((v) => ident(v))
        .join('.')

/**
 * Create a select sql query with bindings for the given table & filters.
 */
export function buildSelectQuery(
    table: string,
    filters: Filters,
    options?: SelectOptions
): QueryPayload {
    // Build initial select query.
    const select = `select * from ${identPath(table)}`

    // Type-check filters and handle case where empty.
    const filtersIsArray = Array.isArray(filters)
    const filtersIsObject = !filtersIsArray && typeof filters === 'object'
    if (
        !filters ||
        (filtersIsArray && !filters.length) ||
        (filtersIsObject && !Object.keys(filters).length)
    ) {
        return {
            sql: addSelectOptionsToQuery(select, options),
            bindings: [],
        }
    }

    filters = filtersIsArray ? filters : [filters]

    // Make sure column names have been converted to snake_case.
    filters = filters.map((filter) => {
        const formatted = {}
        for (const propertyName in filter) {
            formatted[toSnakeCase(propertyName)] = filter[propertyName]
        }
        return formatted
    })

    const orStatements: string[] = []
    const values: any = []
    const bindingIndex = { i: 1 }
    for (const inclusiveFilters of filters as StringKeyMap[]) {
        const andStatement = buildAndStatement(inclusiveFilters, values, bindingIndex)
        andStatement?.length && orStatements.push(andStatement)
    }
    if (!orStatements.length) {
        return {
            sql: addSelectOptionsToQuery(select, options),
            bindings: [],
        }
    }

    const whereClause =
        orStatements.length > 1 ? orStatements.map((s) => `(${s})`).join(' or ') : orStatements[0]

    let sql = `${select} where ${whereClause}`

    return {
        sql: addSelectOptionsToQuery(sql, options),
        bindings: values,
    }
}

/**
 * Create an upsert sql query with bindings for the given table & filters.
 */
export function buildUpsertQuery(
    table: string,
    data: StringKeyMap[],
    conflictColumns: string[],
    updateColumns: string[],
    primaryTimestampColumn?: string,
    returning?: string | string[]
): QueryPayload {
    data = Array.isArray(data) ? data : [data]
    const insertColNames = Object.keys(data[0]).sort()
    if (!insertColNames.length) throw 'No values to upsert'

    let sql = `insert into ${identPath(table)} (${insertColNames
        .map(toSnakeCase)
        .map(ident)
        .join(', ')})`

    const placeholders: string[] = []
    const bindings: any[] = []
    let i = 1
    for (const entry of data) {
        const entryPlaceholders: string[] = []
        for (const colName of insertColNames) {
            entryPlaceholders.push(`$${i}`)
            bindings.push(entry[colName])
            i++
        }
        placeholders.push(`(${entryPlaceholders.join(', ')})`)
    }

    sql += ` values ${placeholders.join(', ')} on conflict (${conflictColumns
        .map(toSnakeCase)
        .map(ident)
        .join(', ')})`

    if (!updateColumns.length) {
        sql += ' do nothing'
        return { sql, bindings }
    }

    updateColumns = updateColumns.map(toSnakeCase)
    const updates: string[] = []
    for (const updateColName of updateColumns) {
        updates.push(`${ident(updateColName)} = excluded.${ident(updateColName)}`)
    }
    sql += ` do update set ${updates.join(', ')}`

    if (primaryTimestampColumn) {
        sql += ` where ${identPath([table, primaryTimestampColumn].join('.'))} >= excluded.${ident(
            primaryTimestampColumn
        )}`
    }

    if (returning) {
        const isAll = returning === '*'
        returning = Array.isArray(returning) ? returning : [returning]
        sql += ` returning ${isAll ? '*' : returning.map(toSnakeCase).map(ident).join(', ')}`
    }

    return { sql, bindings }
}

/**
 * Build an inclusive AND group for a WHERE clause.
 * Ex: x = 3 and y > 4 and ...
 */
export function buildAndStatement(
    filtersMap: StringKeyMap,
    values: any[],
    bindingIndex: StringKeyMap
) {
    if (!filtersMap) return null
    let numKeys
    try {
        numKeys = Object.keys(filtersMap).length
    } catch (e) {
        return null
    }
    if (!numKeys) return null

    const statements: string[] = []

    for (const colPath in filtersMap) {
        let value = filtersMap[colPath]
        const isArray = Array.isArray(value)
        const isObject = !isArray && typeof value === 'object'
        const isFilterObject = isObject && value.op && value.hasOwnProperty('value')

        if (
            value === null ||
            value === undefined ||
            (isArray && !value.length) ||
            (isArray && !!value.find((v) => Array.isArray(v))) ||
            (isObject && (!Object.keys(value).length || !isFilterObject))
        ) {
            continue
        }

        const isMultiColComparison = colPath.includes(',')

        let op = FilterOp.EqualTo
        if (isArray && !isMultiColComparison) {
            op = FilterOp.In
        } else if (isFilterObject) {
            op = value.op
            value = value.value
        }

        if (!filterOpValues.has(op)) continue

        let valuePlaceholder
        if (Array.isArray(value)) {
            const valuePlaceholders: string[] = []
            for (const v of value) {
                valuePlaceholders.push(`$${bindingIndex.i}`)
                values.push(v)
                bindingIndex.i++
            }
            valuePlaceholder = `(${valuePlaceholders.join(', ')})`
        } else {
            valuePlaceholder = `$${bindingIndex.i}`
            values.push(value)
            bindingIndex.i++
        }

        const colEntry = isMultiColComparison
            ? `(${colPath.split(',').map(identPath).join(', ')})`
            : identPath(colPath)

        statements.push(`${colEntry} ${op} ${valuePlaceholder}`)
    }

    return statements.join(' and ')
}

function addSelectOptionsToQuery(sql: string, options?: SelectOptions): string {
    options = options || {}
    const orderBy = options.orderBy

    // Order by
    if (orderBy?.column && Object.values(OrderByDirection).includes(orderBy?.direction)) {
        const orderByColumns = Array.isArray(orderBy.column) ? orderBy.column : [orderBy.column]
        sql += ` order by (${orderByColumns.map(toSnakeCase).map(identPath).join(', ')}) ${
            orderBy.direction
        }`
    }

    // Offset
    if (options.hasOwnProperty('offset')) {
        sql += ` offset ${literal(options.offset)}`
    }

    // Limit
    if (options.hasOwnProperty('limit')) {
        sql += ` limit ${literal(options.limit)}`
    }

    return sql
}

function removeAcronymFromCamel(val: string): string {
    val = val || ''

    let formattedVal = ''
    for (let i = 0; i < val.length; i++) {
        const [prevChar, char, nextChar] = [val[i - 1], val[i], val[i + 1]]
        const [prevCharIsUpperCase, charIsUpperCase, nextCharIsUpperCase] = [
            prevChar && prevChar === prevChar.toUpperCase(),
            char && char === char.toUpperCase(),
            nextChar && nextChar === nextChar.toUpperCase(),
        ]

        if (
            prevCharIsUpperCase &&
            charIsUpperCase &&
            (nextCharIsUpperCase || i === val.length - 1)
        ) {
            formattedVal += char.toLowerCase()
        } else {
            formattedVal += char
        }
    }

    return formattedVal
}

function toSnakeCase(value: string): string {
    return humps.decamelize(removeAcronymFromCamel(value))
}
