export type StringKeyMap = { [key: string]: any }

export type StringMap = { [key: string]: string }

export type SpecTableClientOptions = {
    origin?: string
}

export type QueryPayload = {
    schemaName: string
    tableName: string
    sql: string
    bindings: any[] | null
}

export enum FilterOp {
    EqualTo = '=',
    NotEqualTo = '!=',
    GreaterThan = '>',
    GreaterThanOrEqualTo = '>=',
    LessThan = '<',
    LessThanOrEqualTo = '<=',
    In = 'in',
    NotIn = 'not in',
}

export interface Filter {
    op: FilterOp
    value: any
}

export type Filters = StringKeyMap | StringKeyMap[]

export enum OrderByDirection {
    ASC = 'asc',
    DESC = 'desc',
}

export type OrderBy = {
    column: string | string[]
    direction: OrderByDirection
}

export type SelectOptions = {
    orderBy?: OrderBy
    offset?: number
    limit?: number
    chainId?: string
    blockRange?: number[]
}

export type AuthOptions = {
    token: string | null
}
