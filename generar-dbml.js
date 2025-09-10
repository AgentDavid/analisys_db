require('dotenv').config();
const { Pool } = require('pg');
let GoogleGenerativeAI = null; // carga diferida
const fs = require('fs');
const { Parser } = require('@dbml/core');

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined
});

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
// Eliminamos generación de archivo separado de relaciones
// const RELS_FILE = process.env.RELS_FILE || 'database_relaciones.dbml';
const SAMPLE_SIZE = Number(process.env.SAMPLE_SIZE || 40);
const MAX_VALUE_LENGTH = Number(process.env.MAX_VALUE_LENGTH || 250);
const TABLE_CONCURRENCY = Number(process.env.TABLE_CONCURRENCY || 4);
const GEMINI_MAX_RETRIES = Number(process.env.GEMINI_MAX_RETRIES || 3);
const GEMINI_RETRY_BASE_MS = Number(process.env.GEMINI_RETRY_BASE_MS || 1500);
const SAVE_EVERY = Number(process.env.SAVE_EVERY || 15);
const INCLUDE_FK_GUESS = (process.env.INCLUDE_FK_GUESS || 'true') === 'true';
const INCLUDE_GUESSED_REFS = (process.env.INCLUDE_GUESSED_REFS || 'true') === 'true'; // genera Ref: inferidos si no hay FKs reales
const INFER_FKS = (process.env.INFER_FKS || 'true') === 'true'; // intenta inferir relaciones reales por coincidencia de valores
const FK_INFER_SAMPLE = Number(process.env.FK_INFER_SAMPLE || 500); // filas distintas por columna para comparar
const FK_INFER_MIN_RATIO = Number(process.env.FK_INFER_MIN_RATIO || 0.85); // ratio mínimo de coincidencia
const FK_INFER_MIN_MATCHES = Number(process.env.FK_INFER_MIN_MATCHES || 5); // mínimo de valores coincidentes
const FK_INFER_MAX_TARGETS = Number(process.env.FK_INFER_MAX_TARGETS || 5); // evita explosión combinatoria
const GROUP_MIN_SIZE = Number(process.env.GROUP_MIN_SIZE || 3);
const GROUP_PREFIX_MIN_LEN = Number(process.env.GROUP_PREFIX_MIN_LEN || 3);
const GROUP_AI_DESCRIPTIONS = (process.env.GROUP_AI_DESCRIPTIONS || 'true') === 'true';
// Enriquecimiento eliminado: siempre genera desde cero
const SELECTIVE_RETRY_ATTEMPTS = Number(process.env.SELECTIVE_RETRY_ATTEMPTS || 2); // reintentos adicionales para columnas faltantes
const NO_ACCESS_REPORT = process.env.NO_ACCESS_REPORT || './outputs/tablas_sin_acceso.json';
const PROGRESS_EVERY = Number(process.env.PROGRESS_EVERY || 5); // frecuencia de log por tablas

// Métricas globales
const metrics = {
    startTime: 0,
    schemas: 0,
    tablesTotal: 0,
    tablesProcessed: 0,
    tablesAI: 0,
    tablesSkipped: 0, // deprecated
    tablesRestricted: 0,
    columnsPlaceholders: 0,
    selectiveRetries: 0,
    groupsCreated: 0
};

// Validación de variables requeridas (todas deben venir definidas al inicio)
function ensureEnv(keys) {
    const missing = keys.filter(k => !process.env[k] || String(process.env[k]).trim() === '');
    if (missing.length) {
        console.error('Faltan variables de entorno requeridas:', missing.join(', '));
        process.exit(1);
    }
}
ensureEnv(['DB_USER', 'DB_HOST', 'DB_DATABASE', 'DB_PASSWORD', 'DB_PORT']);
if (USE_AI) ensureEnv(['GEMINI_API_KEY']);

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
    const q = `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast') AND schema_name NOT LIKE 'pg_temp_%' ORDER BY schema_name`;
    const res = await pool.query(q);
    return res.rows.map(r => r.schema_name);
}

async function getAllTables(schema) {
    const q = `SELECT table_name FROM information_schema.tables WHERE table_schema=$1 AND table_type='BASE TABLE' ORDER BY table_name`;
    const res = await pool.query(q, [schema]);
    return res.rows.map(r => r.table_name);
}

async function getTableSchema(table, schema) {
    const q = `SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`;
    const res = await pool.query(q, [schema, table]);
    return res.rows;
}

async function getTableSampleData(table, schema) {
    try {
        const q = `SELECT * FROM "${schema}"."${table}" LIMIT ${SAMPLE_SIZE}`;
        const res = await pool.query(q);
        return res.rows;
    } catch (e) {
        console.warn(`Sample failed ${schema}.${table}: ${e.message}`);
        return [];
    }
}

async function getForeignKeys(schema) {
    // Usamos pg_constraint para obtener FKs incluyendo claves compuestas y referencias a otros esquemas
    const q = `
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            a.attname AS column_name,
            nf.nspname AS foreign_table_schema,
            cf.relname AS foreign_table_name,
            af.attname AS foreign_column_name,
            con.conname AS constraint_name,
            ord.ordinality AS position
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class cf ON cf.oid = con.confrelid
        JOIN pg_namespace nf ON nf.oid = cf.relnamespace
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ord(attnum, ordinality) ON true
        JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ord.attnum
        JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS ord2(attnum, ordinality) ON ord2.ordinality = ord.ordinality
        JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = ord2.attnum
        WHERE con.contype = 'f' AND n.nspname = $1
        ORDER BY table_name, constraint_name, ord.ordinality`;
    const res = await pool.query(q, [schema]);
    return res.rows;
}

async function getUniqueIndexes(schema) {
    const q = `
            SELECT n.nspname AS schema_name, c.relname AS table_name, i.relname AS index_name,
                         pg_get_indexdef(i.oid) AS index_def,
                         ix.indisunique AS is_unique, ix.indisprimary AS is_primary
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class c ON c.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND ix.indisunique = true AND ix.indisprimary = false
            ORDER BY c.relname, i.relname`;
    const res = await pool.query(q, [schema]);
    return res.rows;
}

async function getCheckConstraints(schema) {
    const q = `
            SELECT n.nspname AS schema_name,
                         c.relname AS table_name,
                         con.conname AS constraint_name,
                         pg_get_constraintdef(con.oid, true) AS constraint_def
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype = 'c' AND n.nspname = $1
            ORDER BY c.relname, con.conname`;
    const res = await pool.query(q, [schema]);
    return res.rows;
}

async function getPrimaryKeys(schema) {
    const q = `SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
              WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
              ORDER BY tc.table_name, kcu.ordinal_position`;
    const res = await pool.query(q, [schema]);
    const pk = {};
    for (const r of res.rows) {
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
            // patrones de nombre
            let base = null;
            if (lc.endsWith('_id')) base = lc.slice(0, -3); else if (lc.startsWith('id_')) base = lc.slice(3);
            if (!base || base.length < 2) continue;
            // candidatos target: exacto, plural, singular
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
                // Ejecutar comparación por valores (subset ratio) sobre una muestra
                try {
                    const subquery = `SELECT DISTINCT ${cname} AS val FROM "${schema}"."${tm.table}" WHERE ${cname} IS NOT NULL LIMIT ${FK_INFER_SAMPLE}`;
                    const q = `WITH src AS (${subquery})
                               SELECT (SELECT COUNT(*) FROM src) AS total,
                                      (SELECT COUNT(*) FROM src s JOIN "${schema}"."${target.table}" t ON t."${targetId}" = s.val) AS matched,
                                      (SELECT COUNT(DISTINCT t."${targetId}") FROM src s JOIN "${schema}"."${target.table}" t ON t."${targetId}" = s.val) AS distinct_matched`;
                    const r = await pool.query(q);
                    const row = r.rows[0];
                    const total = Number(row.total);
                    const matched = Number(row.matched);
                    const ratio = total ? matched / total : 0;
                    if (matched >= FK_INFER_MIN_MATCHES && ratio >= FK_INFER_MIN_RATIO) {
                        inferred.push({ table: tm.table, column: cname, target_table: target.table, target_column: targetId, ratio, matched, total });
                        existingFkSet.add(fkKey);
                        break; // ya inferido para esta columna
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

function mapType(pgType) {
    const t = pgType.toLowerCase();
    if (t.includes('character varying')) return 'varchar';
    if (t === 'character' || t === 'char') return 'char';
    if (t === 'timestamp without time zone') return 'timestamp';
    if (t === 'timestamp with time zone') return 'timestamptz';
    // Normalizamos tipos TIME a timestamp para evitar 'time without time zone' inválido en DBML generado
    if (t === 'time without time zone' || t === 'time with time zone' || t === 'time') return 'timestamp';
    if (t === 'double precision') return 'double';
    if (t === 'integer') return 'int';
    if (t === 'bigint') return 'bigint';
    if (t === 'smallint') return 'smallint';
    if (t.startsWith('json')) return 'json';
    return pgType;
}

function escapeNote(s) {
    if (s === null || s === undefined) return '';
    if (typeof s !== 'string') {
        try { s = String(s); } catch (e) { s = ''; }
    }
    return s.replace(/\r?\n+/g, ' ').replace(/'/g, "\\'").slice(0, 800);
}

function stringifyNote(note) {
    // Normalize any structure returned by AI into a concise English sentence
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
    // Quitar paréntesis externos redundantes
    if (/^\((.*)\)$/.test(v)) {
        const inner = v.replace(/^\((.*)\)$/, '$1');
        if (inner.indexOf(')') > inner.indexOf('(')) { } else v = inner; // heurística básica
    }
    // nextval -> increment
    if (/^nextval\(/i.test(v)) return { type: 'increment' };
    // Quitar casts ::
    if (/::[a-zA-Z_][a-zA-Z0-9_\s"\.]*$/.test(v)) {
        v = v.replace(/::[a-zA-Z_][a-zA-Z0-9_\s"\.]*$/, '');
    }
    // Literal string con comillas simples
    let mStr = v.match(/^'(.*)'$/s);
    if (mStr) {
        return { type: 'default', value: `'${mStr[1].replace(/'/g, "''")}'` };
    }
    // String quoted in double quotes inside quotes pattern '(text)'::character varying
    const innerCast = v.match(/^'(.*)'$/s);
    if (innerCast) return { type: 'default', value: `'${innerCast[1].replace(/'/g, "''")}'` };
    // Numeric
    if (/^[0-9]+(\.[0-9]+)?$/.test(v)) return { type: 'default', value: v };
    // Boolean/null
    if (/^(true|false|null)$/i.test(v)) return { type: 'default', value: v.toLowerCase() };
    // Funciones típicas -> expresión
    if (/^[a-zA-Z_][a-zA-Z0-9_]*\(.*\)$/.test(v)) return { type: 'default', value: `\`${v}\`` };
    // Fallback: tratar como string si contiene letras y no espacios raros
    if (/^[A-Z0-9_]+$/i.test(v)) return { type: 'default', value: `'${v.replace(/'/g, "''")}'` };
    return { type: 'default', value: `\`${v}\`` }; // última opción expresión
}

function isSimpleIdentifier(name) {
    return /^[a-z_][a-z0-9_]*$/.test(name);
}

function formatTableName(schema, table) {
    const s = isSimpleIdentifier(schema) ? schema : `"${schema}"`;
    const t = isSimpleIdentifier(table) ? table : `"${table}"`;
    return `${s}.${t}`;
}

function formatRef(schema, table, column) {
    const sch = isSimpleIdentifier(schema) ? schema : `"${schema}"`;
    const tbl = isSimpleIdentifier(table) ? table : `"${table}"`;
    const col = isSimpleIdentifier(column) ? column : `"${column}"`;
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
        // Rellena placeholders si aún faltan
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
        if (e.code === '42501' || /permission denied/i.test(e.message)) {
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
        if (e.code === '42501' || /permission denied/i.test(e.message)) {
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
    const pkCols = new Set(compositePk ? [] : pkColsArr); // sólo marcar pk si es single
    for (const col of columns) {
        const colNameFmt = isSimpleIdentifier(col.column_name) ? col.column_name : `"${col.column_name}"`;
        let line = `  ${colNameFmt} ${mapType(col.data_type)}`;
        const settings = [];
        if (col.column_default) {
            const def = formatDefaultValue(col.column_default, col.data_type);
            if (def) {
                if (def.type === 'increment') settings.push('increment');
                else if (def.type === 'default') settings.push(`default: ${def.value}`);
            }
        }
        if (col.is_nullable.toUpperCase() === 'NO') settings.push('not null');
        if (pkCols.has(col.column_name)) settings.push('pk');
        if (USE_AI) {
            const aiDescRaw = ai.columns[col.column_name];
            const aiDesc = stringifyNote(aiDescRaw);
            if (aiDesc) settings.push(`note: '${escapeNote(aiDesc)}'`);
        }
        // Eliminamos inline ref para evitar sintaxis inválida y duplicados, relaciones se listan con Ref: posteriormente
        if (settings.length) line += ` [${settings.join(', ')}]`;
        tableStr += line + '\n';
    }
    // Índices (composite PK y únicos) se inyectarán después externamente mediante placeholders especiales
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

// Eliminadas funciones de enriquecimiento incremental

async function main() {
    const started = Date.now();
    let dbml = '';
    global.noAccessTables = [];
    metrics.startTime = started;
    const emittedRefs = new Set(); // evita duplicados de relaciones
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
    try {
        const schemas = await getAllUserSchemas();
        metrics.schemas = schemas.length;
        const knownTables = new Set();
        const columnsMapGlobal = {}; // schema.table => Set(columns) para validar refs destino
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
            const fkIndex = {}; // (no usado ya para inline ref pero mantenido por compatibilidad)
            for (const fk of fks) {
                const key = `${schema}.${fk.table_name}.${fk.column_name}`;
                if (!fkIndex[key]) fkIndex[key] = [];
                fkIndex[key].push(fk);
            }
            // Mapas auxiliares para índices únicos y checks por tabla
            const uniqueMap = {};
            for (const u of uniques) {
                const m = u.index_def.match(/\(([^)]+)\)/);
                const cols = m ? m[1].split(',').map(s => s.trim().replace(/"/g, '')) : [];
                if (!cols.length) continue;
                if (!uniqueMap[u.table_name]) uniqueMap[u.table_name] = [];
                uniqueMap[u.table_name].push(cols);
            }
            const checkMap = {};
            for (const c of checks) {
                if (!checkMap[c.table_name]) checkMap[c.table_name] = [];
                checkMap[c.table_name].push(c.constraint_def.replace(/^CHECK\s*/i, '').trim());
            }
            let processed = 0;
            const queue = [...tables];
            const tableMetas = [];
            const columnsMap = {}; // tabla completa -> set de columnas para validación refs (por esquema)
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
                        const meta = await processTable(schema, t, fkIndex, pkMap);
                        // Inyección de indexes / checks dentro del bloque de la tabla
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
                    processed++;
                    if (processed % SAVE_EVERY === 0) {
                        fs.writeFileSync(PARTIAL_FILE, dbml);
                        console.log(`Guardado parcial (${processed}/${tables.length} tablas en ${schema})`);
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
                    if (!knownTables.has(`${srcSchema}.${srcTable}`) || !knownTables.has(`${dstSchema}.${dstTable}`)) continue; // evitar tablas inexistentes
                    const srcColsSetLocal = columnsMap[`${srcSchema}.${srcTable}`] || new Set();
                    const dstColsSetGlobal = columnsMapGlobal[`${dstSchema}.${dstTable}`]; // puede ser undefined si aún no se procesó
                    // Preparar pares ordenados (col_local, col_destino)
                    const orderedPairs = [];
                    const seenPair = new Set();
                    for (const r of rows) {
                        const key = r.column_name + '>' + r.foreign_column_name;
                        if (seenPair.has(key)) continue;
                        seenPair.add(key);
                        orderedPairs.push([r.column_name, r.foreign_column_name]);
                    }
                    // Filtrar por existencia de columna local y (si disponible) destino
                    const validPairs = orderedPairs.filter(([s, d]) => srcColsSetLocal.has(s) && (!dstColsSetGlobal || dstColsSetGlobal.has(d)));
                    if (!validPairs.length) continue; // nada válido
                    if (validPairs.length === 1) {
                        const [s, d] = validPairs[0];
                        // Evitar auto-ref exacta misma columna
                        if (srcSchema === dstSchema && srcTable === dstTable && s === d) continue;
                        const k = refKey(srcSchema, srcTable, [s], dstSchema, dstTable, [d]);
                        if (emittedRefs.has(k)) continue;
                        emittedRefs.add(k);
                        dbml += `Ref: ${formatRef(srcSchema, srcTable, s)} > ${formatRef(dstSchema, dstTable, d)}\n`;
                        countLines++;
                        continue;
                    }
                    // Verificar cardinalidad: número local == número destino
                    const localCols = validPairs.map(p => p[0]);
                    const dstCols = validPairs.map(p => p[1]);
                    if (localCols.length !== dstCols.length) {
                        // degradar a refs simples por par
                        for (const [s, d] of validPairs.slice(0, Math.min(localCols.length, dstCols.length))) {
                            if (srcSchema === dstSchema && srcTable === dstTable && s === d) continue;
                            const k = refKey(srcSchema, srcTable, [s], dstSchema, dstTable, [d]);
                            if (emittedRefs.has(k)) continue;
                            emittedRefs.add(k);
                            dbml += `Ref: ${formatRef(srcSchema, srcTable, s)} > ${formatRef(dstSchema, dstTable, d)}\n`;
                            countLines++;
                        }
                        continue;
                    }
                    // Relación compuesta válida
                    // Evitar caso donde todas las columnas son idénticas en ambos lados (crearía endpoints iguales)
                    if (srcSchema === dstSchema && srcTable === dstTable && localCols.length === dstCols.length && localCols.every((c, i) => c === dstCols[i])) {
                        // degradar a nada o individuales (pero serían iguales); optamos por omitir
                        continue;
                    }
                    const compositeKey = refKey(srcSchema, srcTable, localCols, dstSchema, dstTable, dstCols);
                    if (emittedRefs.has(compositeKey)) continue;
                    emittedRefs.add(compositeKey);
                    const leftCols = localCols.map(c => isSimpleIdentifier(c) ? c : `"${c}"`).join(', ');
                    const rightCols = dstCols.map(c => isSimpleIdentifier(c) ? c : `"${c}"`).join(', ');
                    dbml += `Ref: ${isSimpleIdentifier(srcSchema) ? srcSchema : `"${srcSchema}"`}.${isSimpleIdentifier(srcTable) ? srcTable : `"${srcTable}"`}.(${leftCols}) > ${isSimpleIdentifier(dstSchema) ? dstSchema : `"${dstSchema}"`}.${isSimpleIdentifier(dstTable) ? dstTable : `"${dstTable}"`}.(${rightCols})\n`;
                    countLines++;
                }
                dbml += '\n';
                console.log(`   [FK] ${countLines} relaciones añadidas en ${schema} (constraints=${Object.keys(byConstraint).length})`);
            }

            // Referencias inferidas heurística simple (solo si no activamos inferencia por datos)
            if (!INFER_FKS && INCLUDE_GUESSED_REFS) {
                const existingRefCols = new Set(fks.map(f => `${f.table_name}.${f.column_name}`));
                for (const tm of tableMetas) {
                    if (!tm.columnsRaw) continue; // columnas crudas devueltas por processTable (tras cambio más abajo si aún no existen)
                    for (const col of tm.columnsRaw) {
                        const cname = col.column_name.toLowerCase();
                        if (existingRefCols.has(`${tm.table}.${col.column_name}`)) continue;
                        if (cname === 'id') continue;
                        let base = null;
                        if (cname.endsWith('_id')) base = cname.slice(0, -3); else if (cname.startsWith('id_')) base = cname.slice(3);
                        if (!base || base.length < 2) continue;
                        // Candidatos de tabla
                        const candidates = [base, `${base}s`, base.endsWith('s') ? base.slice(0, -1) : null].filter(Boolean);
                        let targetTable = candidates.find(c => tables.includes(c));
                        if (!targetTable) continue; // misma lista 'tables' del esquema actual
                        // Evitar autoref no intencional
                        if (targetTable === tm.table) continue;
                        const k = refKey(schema, tm.table, [col.column_name], schema, targetTable, ['id']);
                        if (!emittedRefs.has(k)) {
                            emittedRefs.add(k);
                            const line = `Ref: ${formatRef(schema, tm.table, col.column_name)} > ${formatRef(schema, targetTable, 'id')} // guessed`;
                            dbml += line + '\n';
                        }
                    }
                }
                dbml += '\n';
            }

            // Inferencia basada en datos
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

            // Índices únicos y checks ahora se incluyen dentro de cada tabla, no se generan bloques globales.
        }
        console.log('\nValidando DBML final...');
        const valid = await validateDbml(dbml);
        fs.writeFileSync(valid ? OUTPUT_FILE : ERROR_OUTPUT_FILE, dbml);
        // Eliminado archivo separado de relaciones
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
        console.log(`Tablas saltadas (ya enriquecidas): ${metrics.tablesSkipped}`);
        console.log(`Tablas restringidas: ${metrics.tablesRestricted}`);
        console.log(`Columnas con placeholder: ${metrics.columnsPlaceholders}`);
        console.log(`Reintentos selectivos: ${metrics.selectiveRetries}`);
        console.log(`Grupos creados: ${metrics.groupsCreated}`);
        console.log(`Validación: ${valid ? 'OK' : 'FALLÓ'} -> Archivo: ${valid ? OUTPUT_FILE : ERROR_OUTPUT_FILE}`);
        console.log(`Tiempo total: ${totalTime}s`);
        console.log('================');
    } catch (e) {
        console.error('Fallo global:', e);
        fs.writeFileSync(ERROR_OUTPUT_FILE, dbml);
    } finally {
        await pool.end();
    }
}

if (require.main === module) main();