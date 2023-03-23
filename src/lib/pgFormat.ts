// reserved Postgres words
var reservedMap = {
    AES128: true,
    AES256: true,
    ALL: true,
    ALLOWOVERWRITE: true,
    ANALYSE: true,
    ANALYZE: true,
    AND: true,
    ANY: true,
    ARRAY: true,
    AS: true,
    ASC: true,
    AUTHORIZATION: true,
    BACKUP: true,
    BETWEEN: true,
    BINARY: true,
    BLANKSASNULL: true,
    BOTH: true,
    BYTEDICT: true,
    CASE: true,
    CAST: true,
    CHECK: true,
    COLLATE: true,
    COLUMN: true,
    CONSTRAINT: true,
    CREATE: true,
    CREDENTIALS: true,
    CROSS: true,
    CURRENT_DATE: true,
    CURRENT_TIME: true,
    CURRENT_TIMESTAMP: true,
    CURRENT_USER: true,
    CURRENT_USER_ID: true,
    DEFAULT: true,
    DEFERRABLE: true,
    DEFLATE: true,
    DEFRAG: true,
    DELTA: true,
    DELTA32K: true,
    DESC: true,
    DISABLE: true,
    DISTINCT: true,
    DO: true,
    ELSE: true,
    EMPTYASNULL: true,
    ENABLE: true,
    ENCODE: true,
    ENCRYPT: true,
    ENCRYPTION: true,
    END: true,
    EXCEPT: true,
    EXPLICIT: true,
    FALSE: true,
    FOR: true,
    FOREIGN: true,
    FREEZE: true,
    FROM: true,
    FULL: true,
    GLOBALDICT256: true,
    GLOBALDICT64K: true,
    GRANT: true,
    GROUP: true,
    GZIP: true,
    HAVING: true,
    IDENTITY: true,
    IGNORE: true,
    ILIKE: true,
    IN: true,
    INITIALLY: true,
    INNER: true,
    INTERSECT: true,
    INTO: true,
    IS: true,
    ISNULL: true,
    JOIN: true,
    LEADING: true,
    LEFT: true,
    LIKE: true,
    LIMIT: true,
    LOCALTIME: true,
    LOCALTIMESTAMP: true,
    LUN: true,
    LUNS: true,
    LZO: true,
    LZOP: true,
    MINUS: true,
    MOSTLY13: true,
    MOSTLY32: true,
    MOSTLY8: true,
    NATURAL: true,
    NEW: true,
    NOT: true,
    NOTNULL: true,
    NULL: true,
    NULLS: true,
    OFF: true,
    OFFLINE: true,
    OFFSET: true,
    OLD: true,
    ON: true,
    ONLY: true,
    OPEN: true,
    OR: true,
    ORDER: true,
    OUTER: true,
    OVERLAPS: true,
    PARALLEL: true,
    PARTITION: true,
    PERCENT: true,
    PLACING: true,
    PRIMARY: true,
    RAW: true,
    READRATIO: true,
    RECOVER: true,
    REFERENCES: true,
    REJECTLOG: true,
    RESORT: true,
    RESTORE: true,
    RIGHT: true,
    SELECT: true,
    SESSION_USER: true,
    SIMILAR: true,
    SOME: true,
    SYSDATE: true,
    SYSTEM: true,
    TABLE: true,
    TAG: true,
    TDES: true,
    TEXT255: true,
    TEXT32K: true,
    THEN: true,
    TO: true,
    TOP: true,
    TRAILING: true,
    TRUE: true,
    TRUNCATECOLUMNS: true,
    UNION: true,
    UNIQUE: true,
    USER: true,
    USING: true,
    VERBOSE: true,
    WALLET: true,
    WHEN: true,
    WHERE: true,
    WITH: true,
    WITHOUT: true,
}

var fmtPattern = {
    ident: 'I',
    literal: 'L',
    string: 's',
}

// convert to Postgres default ISO 8601 format
function formatDate(date) {
    date = date.replace('T', ' ')
    date = date.replace('Z', '+00')
    return date
}

function isReserved(value) {
    if (reservedMap[value.toUpperCase()]) {
        return true
    }
    return false
}

function arrayToList(useSpace, array, formatter) {
    var sql = ''
    var temp = []

    sql += useSpace ? ' (' : '('
    for (var i = 0; i < array.length; i++) {
        sql += (i === 0 ? '' : ', ') + formatter(array[i])
    }
    sql += ')'

    return sql
}

// Ported from PostgreSQL 9.2.4 source code in src/interfaces/libpq/fe-exec.c
export function ident(value) {
    if (value === undefined || value === null) {
        throw new Error('SQL identifier cannot be null or undefined')
    } else if (value === false) {
        return '"f"'
    } else if (value === true) {
        return '"t"'
    } else if (value instanceof Date) {
        return '"' + formatDate(value.toISOString()) + '"'
    } else if (Array.isArray(value) === true) {
        var temp = []
        for (var i = 0; i < value.length; i++) {
            if (Array.isArray(value[i]) === true) {
                throw new Error(
                    'Nested array to grouped list conversion is not supported for SQL identifier'
                )
            } else {
                // @ts-ignore
                temp.push(ident(value[i]))
            }
        }
        return temp.toString()
    } else if (value === Object(value)) {
        throw new Error('SQL identifier cannot be an object')
    }

    var ident = value.toString().slice(0) // create copy

    // do not quote a valid, unquoted identifier
    if (/^[a-z_][a-z0-9_$]*$/.test(ident) === true && isReserved(ident) === false) {
        return ident
    }

    var quoted = '"'

    for (var i = 0; i < ident.length; i++) {
        var c = ident[i]
        if (c === '"') {
            quoted += c + c
        } else {
            quoted += c
        }
    }

    quoted += '"'

    return quoted
}

// Ported from PostgreSQL 9.2.4 source code in src/interfaces/libpq/fe-exec.c
export function literal(value) {
    var lit = null
    var explicitCast = null

    if (value === undefined || value === null) {
        return 'NULL'
    } else if (value === false) {
        return "'f'"
    } else if (value === true) {
        return "'t'"
    } else if (value instanceof Date) {
        return "'" + formatDate(value.toISOString()) + "'"
    } else if (Array.isArray(value) === true) {
        var temp = []
        for (var i = 0; i < value.length; i++) {
            if (Array.isArray(value[i]) === true) {
                // @ts-ignore
                temp.push(arrayToList(i !== 0, value[i], lit))
            } else {
                // @ts-ignore
                temp.push(literal(value[i]))
            }
        }
        return temp.toString()
    } else if (value === Object(value)) {
        // @ts-ignore
        explicitCast = 'jsonb'
        // @ts-ignore
        lit = JSON.stringify(value)
    } else {
        lit = value.toString().slice(0) // create copy
    }

    var hasBackslash = false
    var quoted = "'"

    // @ts-ignore
    for (var i = 0; i < lit.length; i++) {
        // @ts-ignore
        var c = lit[i]
        if (c === "'") {
            quoted += c + c
        } else if (c === '\\') {
            quoted += c + c
            hasBackslash = true
        } else {
            quoted += c
        }
    }

    quoted += "'"

    if (hasBackslash === true) {
        quoted = 'E' + quoted
    }

    if (explicitCast) {
        quoted += '::' + explicitCast
    }

    return quoted
}
