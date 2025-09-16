// generar-dbml-azure.js

require('dotenv').config();
const sql = require('mssql');
let GoogleGenerativeAI = null; // carga diferida
const fs = require('fs');
const { Parser } = require('@dbml/core');

// Configuración para Microsoft SQL Server / Azure SQL
const _dbHost = process.env.DB_HOST || '';
const _isIpAddress = /^(?:\d{1,3}\.){3}\d{1,3}$/.test(_dbHost);
if (_isIpAddress && (process.env.DB_TRUST_SERVER_CERTIFICATE === undefined)) {
    console.warn(`DB_HOST appears to be an IP address (${_dbHost}). If the server uses a self-signed certificate, set DB_TRUST_SERVER_CERTIFICATE=true in your .env to allow the connection.`);
}
const sqlConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: _dbHost,
    database: process.env.DB_DATABASE,
    port: Number(process.env.DB_PORT || 1433),
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    },
    options: {
        // encrypt can be disabled via DB_SSL=false (not recommended for production)
        encrypt: (process.env.DB_SSL || 'true').toLowerCase() !== 'false',
        // trustServerCertificate can be explicitly set via DB_TRUST_SERVER_CERTIFICATE=true
        // If DB_TRUST_SERVER_CERTIFICATE is not set and the host is an IP, default to true (convenience for internal/self-signed servers)
        trustServerCertificate: (process.env.DB_TRUST_SERVER_CERTIFICATE !== undefined)
            ? (process.env.DB_TRUST_SERVER_CERTIFICATE || 'false').toLowerCase() === 'true'
            : _isIpAddress,
        // Optional: set a DNS server name to avoid TLS ServerName/IP deprecation warnings
        // Provide DB_SERVER_NAME in .env if your SQL server has a DNS name (e.g. mydb.example.com)
        serverName: process.env.DB_SERVER_NAME || undefined
    }
};

let pool; // El pool se inicializará y conectará en main()

const USE_AI = (process.env.USE_AI || 'false').toLowerCase() === 'true';
let modelName = process.env.GEMINI_MODEL || 'gemini-2.5-flash';
let model = null;
if (USE_AI) {
    try {
        GoogleGenerativeAI = require('@google/generative-ai').GoogleGenerativeAI;
        const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');
        model = genAI.getGenerativeModel({ model: modelName });
    } catch (e) {
        console.error('Error inicializando IA (Gemini). Use VARIANT=plain o defina GEMINI_API_KEY. Detalle:', e.message);
        process.exit(1);
    }
}

const OUTPUT_FILE = process.env.OUTPUT_FILE || './outputs/database_completa.dbml';
const ERROR_OUTPUT_FILE = process.env.ERROR_OUTPUT_FILE || './outputs/database_invalida.dbml';
const PARTIAL_FILE = process.env.PARTIAL_FILE || './outputs/database_parcial.dbml';
const SAMPLE_SIZE = Number(process.env.SAMPLE_SIZE || 40);
const MAX_VALUE_LENGTH = Number(process.env.MAX_VALUE_LENGTH || 250);
const TABLE_CONCURRENCY = Number(process.env.TABLE_CONCURRENCY || 4);
const GEMINI_MAX_RETRIES = Number(process.env.GEMINI_MAX_RETRIES || 3);
const GEMINI_RETRY_BASE_MS = Number(process.env.GEMINI_RETRY_BASE_MS || 1500);
const SAVE_EVERY = Number(process.env.SAVE_EVERY || 15);
const INCLUDE_FK_GUESS = (process.env.INCLUDE_FK_GUESS || 'true') === 'true';
const INCLUDE_GUESSED_REFS = (process.env.INCLUDE_GUESSED_REFS || 'true') === 'true';
const INFER_FKS = (process.env.INFER_FKS || 'true') === 'true';
const FK_INFER_SAMPLE = Number(process.env.FK_INFER_SAMPLE || 500);
const FK_INFER_MIN_RATIO = Number(process.env.FK_INFER_MIN_RATIO || 0.85);
const FK_INFER_MIN_MATCHES = Number(process.env.FK_INFER_MIN_MATCHES || 5);
const FK_INFER_MAX_TARGETS = Number(process.env.FK_INFER_MAX_TARGETS || 5);
const GROUP_MIN_SIZE = Number(process.env.GROUP_MIN_SIZE || 3);
const GROUP_PREFIX_MIN_LEN = Number(process.env.GROUP_PREFIX_MIN_LEN || 3);
const GROUP_AI_DESCRIPTIONS = (process.env.GROUP_AI_DESCRIPTIONS || 'true') === 'true';
const SELECTIVE_RETRY_ATTEMPTS = Number(process.env.SELECTIVE_RETRY_ATTEMPTS || 2);
const NO_ACCESS_REPORT = process.env.NO_ACCESS_REPORT || './outputs/tablas_sin_acceso.json';
const PROGRESS_EVERY = Number(process.env.PROGRESS_EVERY || 5);

const metrics = {
    startTime: 0, schemas: 0, tablesTotal: 0, tablesProcessed: 0, tablesAI: 0, tablesSkipped: 0,
    tablesRestricted: 0, columnsPlaceholders: 0, selectiveRetries: 0, groupsCreated: 0
};

function ensureEnv(keys) {
    const missing = keys.filter(k => !process.env[k] || String(process.env[k]).trim() === '');
    if (missing.length) {
        console.error('Faltan variables de entorno requeridas:', missing.join(', '));
        process.exit(1);
    }
}
ensureEnv(['DB_USER', 'DB_HOST', 'DB_DATABASE', 'DB_PASSWORD']);
if (USE_AI) ensureEnv(['GEMINI_API_KEY']);

// Helper para ejecutar consultas con el driver mssql
async function executeQuery(query, params = {}) {
    const request = pool.request();
    for (const key in params) {
        // El driver mssql puede inferir la mayoría de los tipos a partir del valor de JS
        request.input(key, params[key]);
    }
    const result = await request.query(query);
    return result.recordset;
}

function truncateSampleData(rows) {
    return rows.map(r => {
        const o = {};
        for (const k in r) {
            const v = r[k];
            if (v == null) o[k] = v; else if (typeof v === 'string' && v.length > MAX_VALUE_LENGTH) o[k] = v.slice(0, MAX_VALUE_LENGTH) + '...'; else o[k] = v;
        }
        return o;
    });
}

async function validateDbml(dbmlContent) {
    try {
        Parser.parse(dbmlContent, 'dbml');
        return true;
    } catch (e) {
        console.error('Error validación DBML:', e.message);
        return false;
    }
}

async function getAllUserSchemas() {
    console.log('Listando esquemas...');
    // Intentar usar sys.schemas (más completo) pero puede fallar si el usuario tiene permisos limitados.
    try {
        const q = `SELECT name AS schema_name FROM sys.schemas WHERE name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys') AND name NOT LIKE 'db_%' ORDER BY name;`;
        const res = await executeQuery(q);
        const schemas = res.map(r => r.schema_name).filter(Boolean);
        if (schemas && schemas.length) return schemas;
    } catch (e) {
        console.warn('sys.schemas no accesible o falló: ' + e.message);
    }

    // Fallback: usar information_schema.tables para descubrir esquemas visibles
    try {
        const q2 = `SELECT DISTINCT table_schema AS schema_name FROM information_schema.tables WHERE table_type='BASE TABLE' ORDER BY table_schema;`;
        const res2 = await executeQuery(q2);
        const schemas2 = res2.map(r => r.schema_name).filter(Boolean);
        if (schemas2 && schemas2.length) return schemas2;
    } catch (e) {
        console.warn('information_schema.tables no accesible o falló: ' + e.message);
    }

    // Último recurso: 'dbo'
    console.warn("No se encontraron esquemas visibles. Usando 'dbo' por defecto.");
    return ['dbo'];
}

async function getAllTables(schema) {
    const q = `SELECT table_name FROM information_schema.tables WHERE table_schema=@schema AND table_type='BASE TABLE' ORDER BY table_name`;
    const res = await executeQuery(q, { schema });
    return res.map(r => r.table_name);
}

async function getTableSchema(table, schema) {
    const q = `
        SELECT
            c.name AS column_name,
            t.name AS data_type,
            CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
            d.definition AS column_default,
            ic.is_identity
        FROM sys.columns c
        JOIN sys.objects o ON o.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        JOIN sys.types t ON t.user_type_id = c.user_type_id
        LEFT JOIN sys.default_constraints d ON d.parent_object_id = c.object_id AND d.parent_column_id = c.column_id
        LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE s.name = @schema AND o.name = @table
        ORDER BY c.column_id;`;
    const res = await executeQuery(q, { schema, table });
    return res;
}

async function getTableSampleData(table, schema) {
    try {
        const q = `SELECT TOP ${SAMPLE_SIZE} * FROM [${schema}].[${table}]`;
        const res = await executeQuery(q);
        return res;
    } catch (e) {
        console.warn(`Sample failed ${schema}.${table}: ${e.message}`);
        return [];
    }
}

async function getForeignKeys(schema) {
    const q = `
        SELECT
            s_parent.name AS table_schema,
            t_parent.name AS table_name,
            c_parent.name AS column_name,
            s_ref.name AS foreign_table_schema,
            t_ref.name AS foreign_table_name,
            c_ref.name AS foreign_column_name,
            fk.name AS constraint_name,
            fkc.constraint_column_id AS position
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.objects AS t_parent ON fk.parent_object_id = t_parent.object_id
        INNER JOIN sys.schemas AS s_parent ON t_parent.schema_id = s_parent.schema_id
        INNER JOIN sys.columns AS c_parent ON fkc.parent_object_id = c_parent.object_id AND fkc.parent_column_id = c_parent.column_id
        INNER JOIN sys.objects AS t_ref ON fk.referenced_object_id = t_ref.object_id
        INNER JOIN sys.schemas AS s_ref ON t_ref.schema_id = s_ref.schema_id
        INNER JOIN sys.columns AS c_ref ON fkc.referenced_object_id = c_ref.object_id AND fkc.referenced_column_id = c_ref.column_id
        WHERE s_parent.name = @schema
        ORDER BY table_name, constraint_name, position;`;
    const res = await executeQuery(q, { schema });
    return res;
}

async function getUniqueIndexes(schema) {
    const q = `
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            i.name AS index_name,
            STUFF((
                SELECT ', ' + c.name
                FROM sys.index_columns AS ic
                INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                ORDER BY ic.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS index_def_columns,
            i.is_unique,
            i.is_primary_key AS is_primary
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS t ON i.object_id = t.object_id
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE s.name = @schema AND i.is_unique = 1 AND i.is_primary_key = 0 AND t.type = 'U'
        ORDER BY t.name, i.name;`;
    const res = await executeQuery(q, { schema });
    return res;
}

async function getCheckConstraints(schema) {
    const q = `
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            cc.name AS constraint_name,
            cc.definition AS constraint_def
        FROM sys.check_constraints AS cc
        INNER JOIN sys.objects AS t ON cc.parent_object_id = t.object_id
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE s.name = @schema AND t.type = 'U'
        ORDER BY t.name, cc.name;`;
    const res = await executeQuery(q, { schema });
    return res;
}

async function getPrimaryKeys(schema) {
    const q = `
        SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = @schema
        ORDER BY tc.table_name, kcu.ordinal_position`;
    const res = await executeQuery(q, { schema });
    const pk = {};
    for (const r of res) {
        if (!pk[r.table_name]) pk[r.table_name] = [];
        pk[r.table_name].push(r.column_name);
    }
    return pk;
}


function pickCandidateIdColumns(tableName, columns, pkMap) {
    // Prefer single-column primary key; fallback a columnas llamadas id o <table>_id
    if (pkMap[tableName] && pkMap[tableName].length === 1) return pkMap[tableName];
    const cands = [];
    for (const c of columns) {
        const n = c.column_name.toLowerCase();
        if (n === 'id' || n === `${tableName.toLowerCase()}_id`) cands.push(c.column_name);
    }
    return cands.slice(0, 1); // solo una para simplificar
}

async function inferForeignKeys(schema, tableMetas, pkMap, existingFkSet) {
    const inferred = [];
    const tableIndex = {};
    for (const tm of tableMetas) tableIndex[tm.table] = tm;

    for (const tm of tableMetas) {
        if (!tm.columnsRaw) continue;
        for (const col of tm.columnsRaw) {
            const cname = col.column_name;
            const lc = cname.toLowerCase();
            if (lc === 'id') continue;
            let base = null;
            if (lc.endsWith('_id')) base = lc.slice(0, -3);
            else if (lc.startsWith('id_')) base = lc.slice(3);
            if (!base || base.length < 2) continue;

            const candidates = Array.from(new Set([base, `${base}s`, base.endsWith('s') ? base.slice(0, -1) : null].filter(Boolean)));
            let tested = 0;
            for (const cand of candidates) {
                if (tested >= FK_INFER_MAX_TARGETS) break;
                if (!tableIndex[cand]) continue;
                const target = tableIndex[cand];
                const targetIdCols = pickCandidateIdColumns(target.table, target.columnsRaw || [], pkMap);
                if (!targetIdCols.length) continue;
                const targetId = targetIdCols[0];
                const fkKey = `${tm.table}.${cname}>${target.table}.${targetId}`;
                if (existingFkSet.has(fkKey)) continue;

                try {
                    const subquery = `SELECT DISTINCT TOP ${FK_INFER_SAMPLE} [${cname}] AS val FROM [${schema}].[${tm.table}] WHERE [${cname}] IS NOT NULL`;
                    const q = `WITH src AS (${subquery})
                               SELECT
                                   (SELECT COUNT(*) FROM src) AS total,
                                   (SELECT COUNT(*) FROM src s JOIN [${schema}].[${target.table}] t ON t.[${targetId}] = s.val) AS matched,
                                   (SELECT COUNT(DISTINCT t.[${targetId}]) FROM src s JOIN [${schema}].[${target.table}] t ON t.[${targetId}] = s.val) AS distinct_matched`;
                    const r = await executeQuery(q);
                    const row = r[0];
                    const total = Number(row.total);
                    const matched = Number(row.matched);
                    const ratio = total ? matched / total : 0;
                    if (matched >= FK_INFER_MIN_MATCHES && ratio >= FK_INFER_MIN_RATIO) {
                        inferred.push({ table: tm.table, column: cname, target_table: target.table, target_column: targetId, ratio, matched, total });
                        existingFkSet.add(fkKey);
                        break;
                    }
                } catch (e) {
                    // silencioso
                }
                tested++;
            }
        }
    }
    return inferred;
}

function guessFkNotes(columns) {
    const guesses = {};
    for (const c of columns) {
        const name = c.column_name.toLowerCase();
        if (/(_id|id$)/.test(name) && name !== 'id') guesses[c.column_name] = 'Possible foreign key inferred by column name';
    }
    return guesses;
}

function mapType(sqlServerType) {
    const t = sqlServerType.toLowerCase();
    switch (t) {
        case 'int': return 'int';
        case 'bigint': return 'bigint';
        case 'smallint': return 'smallint';
        case 'tinyint': return 'tinyint';
        case 'bit': return 'boolean';
        case 'decimal': case 'numeric': case 'money': case 'smallmoney': return 'decimal';
        case 'float': case 'real': return 'float';
        case 'date': return 'date';
        case 'datetime': case 'datetime2': case 'smalldatetime': return 'datetime';
        case 'datetimeoffset': return 'datetime';
        case 'time': return 'time';
        case 'char': return 'char';
        case 'varchar': return 'varchar';
        case 'text': return 'text';
        case 'nchar': return 'varchar';
        case 'nvarchar': return 'varchar';
        case 'ntext': return 'text';
        case 'binary': case 'varbinary': return 'varchar';
        case 'image': return 'text';
        case 'uniqueidentifier': return 'varchar';
        case 'xml': return 'xml';
    }
    if (t.startsWith('varchar') || t.startsWith('nvarchar')) return 'varchar';
    if (t.startsWith('char') || t.startsWith('nchar')) return 'char';
    if (t.startsWith('decimal') || t.startsWith('numeric')) return 'decimal';
    return t; // Fallback
}


function escapeNote(s) {
    if (s === null || s === undefined) return '';
    if (typeof s !== 'string') {
        try { s = String(s); } catch (e) { s = ''; }
    }
    return s.replace(/\r?\n+/g, ' ').replace(/'/g, "\\'").slice(0, 800);
}

function stringifyNote(note) {
    if (note == null) return '';
    if (typeof note === 'string') return note.trim();
    if (typeof note === 'object') {
        const parts = [];
        const meaning = note.meaning || note.descripcion || note.description;
        if (meaning) parts.push(String(meaning));
        const unit = note.unit || note.unidad;
        if (unit && String(unit).toLowerCase() !== 'n/a') parts.push(`Unit: ${unit}`);
        const domain = note.domain || note.values || note.dominio;
        if (domain) parts.push(`Domain: ${domain}`);
        const keyInfo = note.foreign_key_or_natural_identifier || note.is_fk_or_ni || note.key || note.identifier;
        if (keyInfo) parts.push(`Key/identifier: ${keyInfo}`);
        return parts.join('. ').trim();
    }
    return String(note);
}

function formatDefaultValue(raw, dataType) {
    if (!raw) return null;
    let v = String(raw).trim();
    while (v.startsWith('(') && v.endsWith(')')) {
        v = v.substring(1, v.length - 1);
    }
    if (/^[a-zA-Z_][a-zA-Z0-9_]*\(.*\)$/.test(v)) {
        return { type: 'default', value: `\`${v}\`` };
    }
    let mStr = v.match(/^(N)?'(.*)'$/s);
    if (mStr) {
        return { type: 'default', value: `'${mStr[2].replace(/'/g, "''")}'` };
    }
    if (/^-?[0-9]+(\.[0-9]+)?$/.test(v)) {
        return { type: 'default', value: v };
    }
    return { type: 'default', value: `\`${v}\`` };
}


function isSimpleIdentifier(name) {
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

function formatTableName(schema, table) {
    const s = isSimpleIdentifier(schema) ? schema : `[${schema}]`;
    const t = isSimpleIdentifier(table) ? table : `[${table}]`;
    return `${s}.${t}`;
}

function formatRef(schema, table, column) {
    const sch = isSimpleIdentifier(schema) ? schema : `[${schema}]`;
    const tbl = isSimpleIdentifier(table) ? table : `[${table}]`;
    const col = isSimpleIdentifier(column) ? column : `[${column}]`;
    return `${sch}.${tbl}.${col}`;
}

async function callGemini(prompt, attempt = 1) {
    if (!USE_AI) throw new Error('IA deshabilitada (VARIANT=plain).');
    try {
        const res = await model.generateContent(prompt);
        const text = res.response.text();
        let cleaned = text.replace(/```json/gi, '').replace(/```/g, '').trim();
        if (!/^\s*\{/.test(cleaned)) {
            const first = cleaned.indexOf('{');
            const last = cleaned.lastIndexOf('}');
            if (first !== -1 && last !== -1 && last > first) cleaned = cleaned.slice(first, last + 1);
        }
        return JSON.parse(cleaned);
    } catch (e) {
        if (attempt >= GEMINI_MAX_RETRIES) throw e;
        const wait = GEMINI_RETRY_BASE_MS * Math.pow(2, attempt - 1);
        console.warn(`Gemini failed attempt ${attempt}. Retrying in ${wait}ms: ${e.message}`);
        await new Promise(r => setTimeout(r, wait));
        return callGemini(prompt, attempt + 1);
    }
}

async function generateCommentsWithGemini(table, schema, columns, sample) {
    if (!USE_AI) return { table_description: '', columns: {} };
    const schemaString = columns.map(c => {
        const flags = [];
        if ((c.is_nullable || '').toUpperCase() === 'NO') flags.push('not null');
        if (c.column_default) {
            const dv = String(c.column_default).replace(/\s+/g, ' ').slice(0, 120);
            flags.push(`default=${dv}`);
        }
        if (c.is_identity) flags.push('identity');
        const extras = flags.length ? `; ${flags.join('; ')}` : '';
        return `${c.column_name} (${c.data_type}${extras})`;
    }).join(', ');

    const truncated = truncateSampleData(sample);
    const sampleString = JSON.stringify(truncated, null, 2);
    const prompt = `You are an expert in relational data modeling. Return valid JSON only.
Table: ${schema}.${table}
Columns: ${schemaString}
Sample (may be truncated, size=${truncated.length}):\n${sampleString}\n
Instructions:
1 Return only valid JSON.
2 Include ALL columns listed above.
3 Provide a concise English description of the table's purpose in the domain.
4 For each column, return a richly detailed, plain-text description in English (2–4 short sentences, a single string value, NOT an object). Cover: business meaning; typical value patterns and examples; unit if relevant; allowed ranges/enums; nullability and default (if any) as hints; whether it appears to be a primary key, foreign key (and likely referenced entity), or a natural/business key.
5 Do NOT include markdown, lists, or JSON objects inside the string. Each value in "columns" must be a STRING.
Format:
{
  "table_description": "...", 
  "columns": { "col1": "One-sentence English description", "col2": "..." }
}`;

    try {
        console.log(`      [AI] Generating descriptions for ${schema}.${table} (cols=${columns.length}, sample=${sample.length})`);
        const initial = await callGemini(prompt);
        if (!initial.columns) initial.columns = {};
        const missing = () => columns.filter(c => !initial.columns[c.column_name]);
        let remaining = missing();
        let attempt = 0;
        while (remaining.length && attempt < SELECTIVE_RETRY_ATTEMPTS) {
            attempt++;
            metrics.selectiveRetries++;
            const needList = remaining.map(c => c.column_name).join(', ');
            console.log(`        [AI] Selective retry #${attempt} missing (${remaining.length}): ${needList}`);
            const followPrompt = `Add only the missing columns in JSON. Table ${schema}.${table}. Missing columns: ${needList}. IMPORTANT: Each value must be a single STRING in English (2–4 short sentences) with detailed meaning, examples, domain, unit (if any), nullability/default hints, and key role (PK/FK/natural). Do NOT return objects. Format: { "columns": { "missingCol": "detailed English description" } }`;
            try {
                const partial = await callGemini(followPrompt);
                if (partial && partial.columns) {
                    for (const k of Object.keys(partial.columns)) {
                        if (!initial.columns[k]) initial.columns[k] = partial.columns[k];
                    }
                }
            } catch (e) {
                // continua; no aborta
            }
            remaining = missing();
        }
        if (remaining.length) {
            for (const c of remaining) {
                initial.columns[c.column_name] = 'Description pending';
                metrics.columnsPlaceholders++;
            }
            console.log(`        [AI] Columns with placeholder: ${remaining.map(c => c.column_name).join(', ')}`);
        }
        if (!initial.table_description) initial.table_description = 'Description pending';
        else metrics.tablesAI++;
        return initial;
    } catch (e) {
        console.warn(`      [AI] Failed generating descriptions for ${schema}.${table}: ${e.message}`);
        return { table_description: 'Not generated', columns: {} };
    }
}


async function processTable(schemaName, tableName, foreignKeysIndex, pkMap) {
    let columns;
    try {
        columns = await getTableSchema(tableName, schemaName);
    } catch (e) {
        if (/permission was denied/i.test(e.message)) {
            const placeholder = `// Access restricted: no privileges to read columns or data\nTable ${formatTableName(schemaName, tableName)} {\n  // no visible columns\n}\n\n`;
            noAccessTables.push({ schema: schemaName, table: tableName, reason: 'permission denied (columns)' });
            metrics.tablesRestricted++;
            return { dbml: placeholder, description: 'Access restricted', table: tableName };
        }
        throw e;
    }
    let sample = [];
    try {
        sample = await getTableSampleData(tableName, schemaName);
    } catch (e) {
        if (/permission was denied/i.test(e.message)) {
            noAccessTables.push({ schema: schemaName, table: tableName, reason: 'permission denied (data)' });
            metrics.tablesRestricted++;
        } else {
            console.warn(`Non-critical sample error ${schemaName}.${tableName}: ${e.message}`);
        }
    }
    let ai = await generateCommentsWithGemini(tableName, schemaName, columns, sample);
    if (!ai.columns) ai.columns = {};
    if (INCLUDE_FK_GUESS) {
        const guesses = guessFkNotes(columns);
        for (const k in guesses) if (!ai.columns[k]) ai.columns[k] = guesses[k];
    }
    let tableStr = '';
    tableStr += `Table ${formatTableName(schemaName, tableName)} {\n`;
    const pkColsArr = pkMap[tableName] || [];
    const compositePk = pkColsArr.length > 1;
    const pkCols = new Set(compositePk ? [] : pkColsArr);
    for (const col of columns) {
        const colNameFmt = isSimpleIdentifier(col.column_name) ? col.column_name : `[${col.column_name}]`;
        let line = `  ${colNameFmt} ${mapType(col.data_type)}`;
        const settings = [];

        if (col.is_identity) {
            settings.push('increment');
        } else if (col.column_default) {
            const def = formatDefaultValue(col.column_default, col.data_type);
            if (def && def.type === 'default') {
                settings.push(`default: ${def.value}`);
            }
        }

        if (col.is_nullable.toUpperCase() === 'NO') settings.push('not null');
        if (pkCols.has(col.column_name)) settings.push('pk');

        if (USE_AI) {
            const aiDescRaw = ai.columns[col.column_name];
            const aiDesc = stringifyNote(aiDescRaw);
            if (aiDesc) settings.push(`note: '${escapeNote(aiDesc)}'`);
        }
        if (settings.length) line += ` [${settings.join(', ')}]`;
        tableStr += line + '\n';
    }
    tableStr += '\n';
    if (USE_AI && ai.table_description) {
        tableStr += `  Note: '${escapeNote(stringifyNote(ai.table_description))}'\n`;
    }
    tableStr += '}\n\n';
    return { dbml: tableStr, description: ai.table_description || '', table: tableName, columnsMeta: ai.columns, columnsRaw: columns };
}


async function generateGroupDescription(schema, groupName, tableMetas) {
    if (!GROUP_AI_DESCRIPTIONS) return 'Grupo de tablas relacionado por prefijo';
    const list = tableMetas.map(t => `{"name":"${t.table}","desc":"${(t.description || '').replace(/"/g, '')}"}`).join(',');
    const prompt = `Eres experto en modelado. Resume el rol conjunto de estas tablas en una frase clara. Devuelve JSON {"group_description":"..."}. Datos: schema=${schema} grupo=${groupName} tablas=[${list}]`;
    try {
        const r = await callGemini(prompt);
        if (r.group_description) return r.group_description;
    } catch (e) { }
    return 'Grupo de tablas relacionado por prefijo';
}

function buildGroups(schema, tableMetas) {
    const groups = {};
    for (const tm of tableMetas) {
        const parts = tm.table.split('_');
        if (parts.length < 2) continue;
        const prefix = parts[0];
        if (prefix.length < GROUP_PREFIX_MIN_LEN) continue;
        if (/^(tbl|tmp|t)$/.test(prefix)) continue;
        if (!groups[prefix]) groups[prefix] = [];
        groups[prefix].push(tm);
    }
    for (const k of Object.keys(groups)) if (groups[k].length < GROUP_MIN_SIZE) delete groups[k];
    return groups;
}


async function main() {
    const started = Date.now();
    let dbml = '';
    global.noAccessTables = [];
    metrics.startTime = started;

    try {
        console.log('Connecting to SQL Server / Azure SQL...');
        pool = new sql.ConnectionPool(sqlConfig);
        await pool.connect();
        console.log('Connection successful.');

        const emittedRefs = new Set();
        function refKey(srcSchema, srcTable, srcColsArr, dstSchema, dstTable, dstColsArr) {
            const left = `${srcSchema}.${srcTable}.(${srcColsArr.join(',')})`;
            const right = `${dstSchema}.${dstTable}.(${dstColsArr.join(',')})`;
            return left + '>' + right;
        }

        console.log('=== CONFIG INICIAL ===');
        console.log(`USE_AI=${USE_AI} MODEL=${USE_AI ? modelName : 'N/A'} CONCURRENCY=${TABLE_CONCURRENCY} SAMPLE_SIZE=${SAMPLE_SIZE}`);
        console.log(`SELECTIVE_RETRY_ATTEMPTS=${SELECTIVE_RETRY_ATTEMPTS} SAVE_EVERY=${SAVE_EVERY}`);
        console.log('======================');

        dbml = `// DBML generado automáticamente\n// Fecha: ${new Date().toISOString()}\n\n`;

        const schemas = await getAllUserSchemas();
        metrics.schemas = schemas.length;
        const knownTables = new Set();
        const columnsMapGlobal = {};
        for (const schema of schemas) {
            dbml += `//=============== ESQUEMA: ${schema} ===============//\n\n`;
            const tables = await getAllTables(schema);
            metrics.tablesTotal += tables.length;
            tables.forEach(t => knownTables.add(`${schema}.${t}`));
            console.log(`
--- Esquema ${schema} (${tables.length} tablas) ---`);

            const fks = await getForeignKeys(schema);
            const uniques = await getUniqueIndexes(schema);
            const checks = await getCheckConstraints(schema);
            const pkMap = await getPrimaryKeys(schema);

            const uniqueMap = {};
            for (const u of uniques) {
                const cols = u.index_def_columns ? u.index_def_columns.split(',').map(s => s.trim()) : [];
                if (!cols.length) continue;
                if (!uniqueMap[u.table_name]) uniqueMap[u.table_name] = [];
                uniqueMap[u.table_name].push(cols);
            }

            const checkMap = {};
            for (const c of checks) {
                if (!checkMap[c.table_name]) checkMap[c.table_name] = [];
                checkMap[c.table_name].push(c.constraint_def);
            }

            const queue = [...tables];
            const tableMetas = [];
            const columnsMap = {};

            async function worker() {
                while (queue.length) {
                    const t = queue.shift();
                    try {
                        const currentGlobal = ++metrics.tablesProcessed;
                        if (currentGlobal % PROGRESS_EVERY === 0) {
                            const pct = ((currentGlobal / metrics.tablesTotal) * 100).toFixed(2);
                            console.log(`   [Progreso Global] ${currentGlobal}/${metrics.tablesTotal} (${pct}%)`);
                        }
                        console.log(`   [Tabla] Procesando ${schema}.${t}`);
                        const meta = await processTable(schema, t, {}, pkMap); // fkIndex no se usa
                        if ((pkMap[t] && pkMap[t].length > 1) || (uniqueMap[t] && uniqueMap[t].length)) {
                            dbml += meta.dbml.replace(/}\n\n$/, '');
                            dbml += '  indexes {\n';
                            if (pkMap[t] && pkMap[t].length > 1) dbml += `    (${pkMap[t].join(', ')}) [pk]\n`;
                            if (uniqueMap[t]) for (const ucols of uniqueMap[t]) dbml += `    (${ucols.join(', ')}) [unique]\n`;
                            dbml += '  }\n';
                            if (checkMap[t]) for (const chk of checkMap[t]) dbml += `  // CHECK ${escapeNote(chk)}\n`;
                            dbml += '}\n\n';
                        } else {
                            dbml += meta.dbml;
                            if (checkMap[t]) dbml = dbml.replace(/}\n\n$/, checkMap[t].map(chk => `  // CHECK ${escapeNote(chk)}\n`).join('') + '}\n\n');
                        }
                        tableMetas.push(meta);
                        const colSet = new Set((meta.columnsRaw || []).map(c => c.column_name));
                        columnsMap[`${schema}.${meta.table}`] = colSet;
                        columnsMapGlobal[`${schema}.${meta.table}`] = colSet;
                    } catch (e) {
                        console.error(`Error tabla ${schema}.${t}: ${e.message}`);
                    }
                    if (metrics.tablesProcessed % SAVE_EVERY === 0) {
                        fs.writeFileSync(PARTIAL_FILE, dbml);
                        console.log(`Guardado parcial (${metrics.tablesProcessed}/${metrics.tablesTotal} tablas)`);
                    }
                }
            }
            const workers = Array.from({ length: Math.min(TABLE_CONCURRENCY, tables.length) }, () => worker());
            await Promise.all(workers);

            if (fks.length) {
                if (!/Relaciones explícitas ${schema}/.test(dbml)) dbml += `// Relaciones explícitas ${schema}\n`;
                const byConstraint = {};
                for (const fk of fks) {
                    if (!byConstraint[fk.constraint_name]) byConstraint[fk.constraint_name] = [];
                    byConstraint[fk.constraint_name].push(fk);
                }
                let countLines = 0;
                for (const cname of Object.keys(byConstraint)) {
                    const rows = byConstraint[cname];
                    if (!rows.length) continue;
                    const srcSchema = rows[0].table_schema;
                    const srcTable = rows[0].table_name;
                    const dstSchema = rows[0].foreign_table_schema;
                    const dstTable = rows[0].foreign_table_name;
                    if (!knownTables.has(`${srcSchema}.${srcTable}`) || !knownTables.has(`${dstSchema}.${dstTable}`)) continue;
                    const srcColsSetLocal = columnsMap[`${srcSchema}.${srcTable}`] || new Set();
                    const dstColsSetGlobal = columnsMapGlobal[`${dstSchema}.${dstTable}`];

                    const orderedPairs = [];
                    const seenPair = new Set();
                    for (const r of rows) {
                        const key = r.column_name + '>' + r.foreign_column_name;
                        if (seenPair.has(key)) continue;
                        seenPair.add(key);
                        orderedPairs.push([r.column_name, r.foreign_column_name]);
                    }

                    const validPairs = orderedPairs.filter(([s, d]) => srcColsSetLocal.has(s) && (!dstColsSetGlobal || dstColsSetGlobal.has(d)));
                    if (!validPairs.length) continue;

                    if (validPairs.length === 1) {
                        const [s, d] = validPairs[0];
                        if (srcSchema === dstSchema && srcTable === dstTable && s === d) continue;
                        const k = refKey(srcSchema, srcTable, [s], dstSchema, dstTable, [d]);
                        if (emittedRefs.has(k)) continue;
                        emittedRefs.add(k);
                        dbml += `Ref: ${formatRef(srcSchema, srcTable, s)} > ${formatRef(dstSchema, dstTable, d)}\n`;
                        countLines++;
                        continue;
                    }

                    const localCols = validPairs.map(p => p[0]);
                    const dstCols = validPairs.map(p => p[1]);

                    if (srcSchema === dstSchema && srcTable === dstTable && localCols.length === dstCols.length && localCols.every((c, i) => c === dstCols[i])) {
                        continue;
                    }

                    const compositeKey = refKey(srcSchema, srcTable, localCols, dstSchema, dstTable, dstCols);
                    if (emittedRefs.has(compositeKey)) continue;
                    emittedRefs.add(compositeKey);
                    const leftCols = localCols.map(c => isSimpleIdentifier(c) ? c : `[${c}]`).join(', ');
                    const rightCols = dstCols.map(c => isSimpleIdentifier(c) ? c : `[${c}]`).join(', ');
                    dbml += `Ref: ${isSimpleIdentifier(srcSchema) ? srcSchema : `[${srcSchema}]`}.${isSimpleIdentifier(srcTable) ? srcTable : `[${srcTable}]`}.(${leftCols}) > ${isSimpleIdentifier(dstSchema) ? dstSchema : `[${dstSchema}]`}.${isSimpleIdentifier(dstTable) ? dstTable : `[${dstTable}]`}.(${rightCols})\n`;
                    countLines++;
                }
                dbml += '\n';
                console.log(`   [FK] ${countLines} relaciones añadidas en ${schema} (constraints=${Object.keys(byConstraint).length})`);
            }

            if (!INFER_FKS && INCLUDE_GUESSED_REFS) {
                // Lógica de inferencia por nombre (sin cambios significativos)
            }

            if (INFER_FKS) {
                console.log(`   [InferFK] Analizando posibles FKs en esquema ${schema} ...`);
                const existingFkSet = new Set();
                for (const fk of fks) existingFkSet.add(`${fk.table_name}.${fk.column_name}>${fk.foreign_table_name}.${fk.foreign_column_name}`);
                const inferred = await inferForeignKeys(schema, tableMetas, pkMap, existingFkSet);
                if (inferred.length) {
                    dbml += `// Relaciones inferidas (heurística valores) ${schema}\n`;
                    for (const inf of inferred) {
                        const k = refKey(schema, inf.table, [inf.column], schema, inf.target_table, [inf.target_column]);
                        if (emittedRefs.has(k)) continue;
                        emittedRefs.add(k);
                        const line = `Ref: ${formatRef(schema, inf.table, inf.column)} > ${formatRef(schema, inf.target_table, inf.target_column)} // inferred ratio=${inf.ratio.toFixed(2)} matched=${inf.matched}/${inf.total}`;
                        dbml += line + '\n';
                    }
                    dbml += '\n';
                    console.log(`   [InferFK] ${inferred.length} relaciones inferidas en ${schema}`);
                } else {
                    console.log(`   [InferFK] 0 relaciones inferidas en ${schema}`);
                }
            }
        }
        console.log('\nValidando DBML final...');
        const valid = await validateDbml(dbml);
        fs.writeFileSync(valid ? OUTPUT_FILE : ERROR_OUTPUT_FILE, dbml);
        try {
            fs.writeFileSync(NO_ACCESS_REPORT, JSON.stringify(noAccessTables, null, 2));
            console.log(`Reporte de tablas sin acceso: ${NO_ACCESS_REPORT} (${noAccessTables.length})`);
        } catch (reportErr) {
            console.warn('No se pudo escribir reporte de tablas sin acceso:', reportErr.message);
        }

        const totalTime = ((Date.now() - started) / 1000).toFixed(2);
        console.log('\n=== RESUMEN ===');
        console.log(`Esquemas: ${metrics.schemas}`);
        console.log(`Tablas totales: ${metrics.tablesTotal}`);
        console.log(`Tablas procesadas: ${metrics.tablesProcessed}`);
        console.log(`Tablas con IA: ${metrics.tablesAI}`);
        console.log(`Tablas restringidas: ${metrics.tablesRestricted}`);
        console.log(`Columnas con placeholder: ${metrics.columnsPlaceholders}`);
        console.log(`Validación: ${valid ? 'OK' : 'FALLÓ'} -> Archivo: ${valid ? OUTPUT_FILE : ERROR_OUTPUT_FILE}`);
        console.log(`Tiempo total: ${totalTime}s`);
        console.log('================');

    } catch (e) {
        console.error('Fallo global:', e);
        fs.writeFileSync(ERROR_OUTPUT_FILE, dbml);
    } finally {
        if (pool) {
            await pool.close();
            console.log('Connection pool closed.');
        }
    }
}

if (require.main === module) main();