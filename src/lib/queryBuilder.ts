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
import { SPEC_SCHEMA, CHAIN_ID_PROPERTY } from './constants'
import { schemaForChainId } from './chains'

const emptyResponseTable = 'ethereum.blocks'
const emptyResponseFilters = [{ number: -1 }]

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
    const blockRange = options?.blockRange || []
    const filtersIsArray = Array.isArray(filters)
    const filtersIsObject = !filtersIsArray && typeof filters === 'object'
    filters = (filtersIsArray ? filters : [filters]).filter((f) => !!f)
    ;[table, filters] = detectSpecSchemaQuery(table, filters as StringKeyMap[], options)
    const [schemaName, tableName] = table.split('.')

    const select = `select * from ${identPath(table)}`

    const filtersIsEmpty =
        !filters ||
        (filtersIsArray && !filters.length) ||
        (filtersIsObject && !Object.keys(filters).length)
    if (filtersIsEmpty && !blockRange.length) {
        return {
            schemaName,
            tableName,
            sql: addSelectOptionsToQuery(select, options),
            bindings: [],
        }
    }

    // Convert column names to snake_case.
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
    if (!orStatements.length && !blockRange.length) {
        return {
            schemaName,
            tableName,
            sql: addSelectOptionsToQuery(select, options),
            bindings: [],
        }
    }

    const whereGroups: string[] = []
    if (blockRange.length) {
        let blockRangeStatement = `block_number >= ${literal(blockRange[0])}`
        if (blockRange.length > 1) {
            blockRangeStatement += ` and block_number <= ${literal(blockRange[1])}`
        }
        whereGroups.push(blockRangeStatement)
    }

    if (orStatements.length) {
        whereGroups.push(
            orStatements.length > 1
                ? orStatements.map((s) => `(${s})`).join(' or ')
                : orStatements[0]
        )
    }

    const whereClause =
        whereGroups.length > 1 ? whereGroups.map((s) => `(${s})`).join(' and ') : whereGroups[0]
    let sql = `${select} where ${whereClause}`

    return {
        schemaName,
        tableName,
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
    const [schemaName, tableName] = table.split('.')

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
        return { schemaName, tableName, sql, bindings }
    }

    updateColumns = updateColumns.map(toSnakeCase)
    const updates: string[] = []
    for (const updateColName of updateColumns) {
        updates.push(`${ident(updateColName)} = excluded.${ident(updateColName)}`)
    }
    sql += ` do update set ${updates.join(', ')}`

    if (primaryTimestampColumn) {
        sql += ` where ${identPath([table, primaryTimestampColumn].join('.'))} <= excluded.${ident(
            primaryTimestampColumn
        )}`
    }

    if (returning) {
        const isAll = returning === '*'
        returning = Array.isArray(returning) ? returning : [returning]
        sql += ` returning ${isAll ? '*' : returning.map(toSnakeCase).map(ident).join(', ')}`
    }

    return { schemaName, tableName, sql, bindings }
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

function detectSpecSchemaQuery(
    table: string,
    filters: StringKeyMap[],
    options?: SelectOptions
): [string, StringKeyMap[]] {
    const [schemaName, tableName] = table.split('.')

    // If not querying the spec schema, just treat things as normal.
    if (schemaName !== SPEC_SCHEMA) return [table, filters]

    // If chain id is included in the options, use this chain id
    // to choose the correct schema to query.
    if (options?.chainId) {
        const chainSchema = schemaForChainId[options.chainId.toString()]
        if (!chainSchema) return [emptyResponseTable, emptyResponseFilters]
        table = [chainSchema, tableName].join('.')
        return [table, filters]
    }

    if (!filters.length) throw `Filters are required when querying the ${SPEC_SCHEMA} schema`

    const chainId = filters[0][CHAIN_ID_PROPERTY]
    if (!chainId) throw `No ${CHAIN_ID_PROPERTY} filter included with ${SPEC_SCHEMA} schema query`

    const chainSchema = schemaForChainId[chainId.toString()]
    if (!chainSchema) return [emptyResponseTable, emptyResponseFilters]

    const allFiltersHaveSameChainId = filters.every((f) => f[CHAIN_ID_PROPERTY] === chainId)
    if (!allFiltersHaveSameChainId) {
        throw `All filters must have equivalent ${CHAIN_ID_PROPERTY} properties`
    }

    const newFilters: StringKeyMap[] = []
    for (const filter of filters as StringKeyMap[]) {
        const newFilter = {}
        for (const key in filter) {
            if (key === CHAIN_ID_PROPERTY) continue
            newFilter[key] = filter[key]
        }
        newFilters.push(newFilter)
    }

    table = [chainSchema, tableName].join('.')
    return [table, newFilters]
}

function toSnakeCase(value: string): string {
    return humps.decamelize(removeAcronymFromCamel(value))
}
