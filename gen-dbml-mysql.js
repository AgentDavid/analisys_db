require('dotenv').config();
const mysql = require('mysql2/promise');
let GoogleGenerativeAI = null; // carga diferida
const fs = require('fs');
const { Parser } = require('@dbml/core');

const connection = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT || 3306,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
    connectionLimit: process.env.TABLE_CONCURRENCY || 4
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

const OUTPUT_FILE = process.env.OUTPUT_FILE || './outputs/database_completa_mysql.dbml';
const ERROR_OUTPUT_FILE = process.env.ERROR_OUTPUT_FILE || './outputs/database_invalida_mysql.dbml';
const PARTIAL_FILE = process.env.PARTIAL_FILE || './outputs/database_parcial_mysql.dbml';
const SAMPLE_SIZE = Number(process.env.SAMPLE_SIZE || 20);
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
const NO_ACCESS_REPORT = process.env.NO_ACCESS_REPORT || './outputs/tablas_sin_acceso_mysql.json';
const PROGRESS_EVERY = Number(process.env.PROGRESS_EVERY || 5);

// Métricas globales
const metrics = {
    startTime: 0,
    databases: 0,
    tablesTotal: 0,
    tablesProcessed: 0,
    tablesAI: 0,
    tablesSkipped: 0,
    tablesRestricted: 0,
    columnsPlaceholders: 0,
    selectiveRetries: 0,
    groupsCreated: 0
};

// Validación de variables requeridas
function ensureEnv(keys) {
    const missing = keys.filter(k => !process.env[k] || String(process.env[k]).trim() === '');
    if (missing.length) {
        console.error('Faltan variables de entorno requeridas:', missing.join(', '));
        process.exit(1);
    }
}
ensureEnv(['DB_USER', 'DB_HOST', 'DB_DATABASE', 'DB_PASSWORD']);
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

async function getAllDatabases() {
    console.log('Listando bases de datos...');
    const q = `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') ORDER BY schema_name`;
    const [rows] = await connection.execute(q);
    return rows.map(r => r.schema_name);
}

async function getAllTables(database) {
    const q = `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name`;
    const [rows] = await connection.execute(q, [database]);
    return rows.map(r => r.table_name);
}

async function getTableSchema(table, database) {
    const q = `SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`;
    const [rows] = await connection.execute(q, [database, table]);
    return rows;
}

async function getTableSampleData(table, database) {
    try {
        const q = `SELECT * FROM \`${database}\`.\`${table}\` LIMIT ?`;
        const [rows] = await connection.execute(q, [SAMPLE_SIZE]);
        return rows;
    } catch (e) {
        console.warn(`Muestra fallida ${database}.${table}: ${e.message}`);
        return [];
    }
}

async function getForeignKeys(database) {
    const q = `
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name,
            kcu.referenced_table_schema,
            kcu.referenced_table_name,
            kcu.referenced_column_name,
            tc.constraint_name,
            kcu.ordinal_position AS position
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND kcu.table_schema = ?
        ORDER BY kcu.table_name, tc.constraint_name, kcu.ordinal_position`;
    const [rows] = await connection.execute(q, [database]);
    return rows;
}

async function getUniqueIndexes(database) {
    const q = `
        SELECT table_name, index_name, column_name, seq_in_index
        FROM information_schema.statistics
        WHERE table_schema = ? AND non_unique = 0 AND index_name != 'PRIMARY'
        ORDER BY table_name, index_name, seq_in_index`;
    const [rows] = await connection.execute(q, [database]);
    // Agrupar por index_name
    const indexes = {};
    for (const r of rows) {
        if (!indexes[r.index_name]) indexes[r.index_name] = { table: r.table_name, columns: [] };
        indexes[r.index_name].columns.push(r.column_name);
    }
    return Object.values(indexes);
}

async function getPrimaryKeys(database) {
    const q = `SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
              WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = ?
              ORDER BY tc.table_name, kcu.ordinal_position`;
    const [rows] = await connection.execute(q, [database]);
    const pk = {};
    for (const r of rows) {
        if (!pk[r.table_name]) pk[r.table_name] = [];
        pk[r.table_name].push(r.column_name);
    }
    return pk;
}

function pickCandidateIdColumns(tableName, columns, pkMap) {
    if (pkMap[tableName] && pkMap[tableName].length === 1) return pkMap[tableName];
    const cands = [];
    for (const c of columns) {
        const n = c.column_name.toLowerCase();
        if (n === 'id' || n === `${tableName.toLowerCase()}_id`) cands.push(c.column_name);
    }
    return cands.slice(0, 1);
}

async function inferForeignKeys(database, tableMetas, pkMap, existingFkSet) {
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
            if (lc.endsWith('_id')) base = lc.slice(0, -3); else if (lc.startsWith('id_')) base = lc.slice(3);
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
                    const subquery = `SELECT DISTINCT ${cname} AS val FROM \`${database}\`.\`${tm.table}\` WHERE ${cname} IS NOT NULL LIMIT ${FK_INFER_SAMPLE}`;
                    const q = `WITH src AS (${subquery})
                               SELECT (SELECT COUNT(*) FROM src) AS total,
                                      (SELECT COUNT(*) FROM src s JOIN \`${database}\`.\`${target.table}\` t ON t.\`${targetId}\` = s.val) AS matched,
                                      (SELECT COUNT(DISTINCT t.\`${targetId}\`) FROM src s JOIN \`${database}\`.\`${target.table}\` t ON t.\`${targetId}\` = s.val) AS distinct_matched`;
                    const [rows] = await connection.execute(q);
                    const row = rows[0];
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
        if (/(_id|id$)/.test(name) && name !== 'id') guesses[c.column_name] = 'Posible clave foránea inferida por nombre';
    }
    return guesses;
}

function mapType(mysqlType) {
    const t = mysqlType.toLowerCase();
    if (t.includes('varchar')) return 'varchar';
    if (t === 'char') return 'char';
    if (t === 'timestamp') return 'timestamp';
    if (t === 'datetime') return 'datetime';
    if (t === 'double') return 'double';
    if (t === 'int') return 'int';
    if (t === 'bigint') return 'bigint';
    if (t === 'smallint') return 'smallint';
    if (t.startsWith('json')) return 'json';
    return mysqlType;
}

function escapeNote(s) {
    if (s === null || s === undefined) return '';
    if (typeof s !== 'string') {
        try { s = JSON.stringify(s); } catch (e) { s = String(s); }
    }
    return s.replace(/\r?\n+/g, ' ').replace(/'/g, "\\'").slice(0, 800);
}

function formatDefaultValue(raw, dataType) {
    if (!raw) return null;
    let v = String(raw).trim();
    if (/^nextval\(/i.test(v)) return { type: 'increment' };
    if (/^\((.*)\)$/.test(v)) {
        const inner = v.replace(/^\((.*)\)$/, '$1');
        if (inner.indexOf(')') > inner.indexOf('(')) { } else v = inner;
    }
    if (/::[a-zA-Z_][a-zA-Z0-9_\s"\.]*$/.test(v)) {
        v = v.replace(/::[a-zA-Z_][a-zA-Z0-9_\s"\.]*$/, '');
    }
    const mStr = v.match(/^'(.*)'$/s);
    if (mStr) {
        return { type: 'default', value: `'${mStr[1].replace(/'/g, "''")}'` };
    }
    if (/^[0-9]+(\.[0-9]+)?$/.test(v)) return { type: 'default', value: v };
    if (/^(true|false|null)$/i.test(v)) return { type: 'default', value: v.toLowerCase() };
    if (/^[a-zA-Z_][a-zA-Z0-9_]*\(.*\)$/.test(v)) return { type: 'default', value: `\`${v}\`` };
    if (/^[A-Z0-9_]+$/i.test(v)) return { type: 'default', value: `'${v.replace(/'/g, "''")}'` };
    return { type: 'default', value: `\`${v}\`` };
}

function isSimpleIdentifier(name) {
    return /^[a-z_][a-z0-9_]*$/.test(name);
}

function formatTableName(database, table) {
    const d = isSimpleIdentifier(database) ? database : `\`${database}\``;
    const t = isSimpleIdentifier(table) ? table : `\`${table}\``;
    return `${d}.${t}`;
}

function formatRef(database, table, column) {
    const d = isSimpleIdentifier(database) ? database : `\`${database}\``;
    const t = isSimpleIdentifier(table) ? table : `\`${table}\``;
    const c = isSimpleIdentifier(column) ? column : `\`${column}\``;
    return `${d}.${t}.${c}`;
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
        console.warn(`Gemini fallo intento ${attempt}. Reintentando en ${wait}ms: ${e.message}`);
        await new Promise(r => setTimeout(r, wait));
        return callGemini(prompt, attempt + 1);
    }
}

async function generateCommentsWithGemini(table, database, columns, sample) {
    if (!USE_AI) return { table_description: '', columns: {} };
    const schemaString = columns.map(c => `${c.column_name} (${c.data_type})`).join(', ');
    const truncated = truncateSampleData(sample);
    const sampleString = JSON.stringify(truncated, null, 2);
    const prompt = `Eres un experto en modelado relacional. Proporciona JSON estricto.
Tabla: ${database}.${table}
Columnas: ${schemaString}
Muestra (puede estar truncada, size=${truncated.length}):\n${sampleString}\n
Instrucciones:
1 Devuelve solo JSON válido.
2 Incluye todas las columnas.
3 Describe el rol de la tabla en el dominio.
4 Para cada columna indica: significado, unidad si aplica, dominio de valores, y si parece clave foránea o identificador natural.
Formato:
{
    "table_description":"...",
    "columns": { "col1":"...", "col2":"..." }
}`;
    try {
        console.log(`      [IA] Generando descripciones tabla ${database}.${table} (cols=${columns.length}, muestra=${sample.length})`);
        const initial = await callGemini(prompt);
        if (!initial.columns) initial.columns = {};
        const missing = () => columns.filter(c => !initial.columns[c.column_name]);
        let remaining = missing();
        let attempt = 0;
        while (remaining.length && attempt < SELECTIVE_RETRY_ATTEMPTS) {
            attempt++;
            metrics.selectiveRetries++;
            const needList = remaining.map(c => c.column_name).join(', ');
            console.log(`        [IA] Reintento selectivo #${attempt} faltan (${remaining.length}): ${needList}`);
            const followPrompt = `Añade solo las columnas faltantes en JSON. Tabla ${database}.${table}. Columnas faltantes: ${needList}. Formato: { "columns": { "colFaltante":"descripcion" } }`;
            try {
                const partial = await callGemini(followPrompt);
                if (partial && partial.columns) {
                    for (const k of Object.keys(partial.columns)) {
                        if (!initial.columns[k]) initial.columns[k] = partial.columns[k];
                    }
                }
            } catch (e) {
                // continua
            }
            remaining = missing();
        }
        if (remaining.length) {
            for (const c of remaining) {
                initial.columns[c.column_name] = 'Descripción pendiente';
                metrics.columnsPlaceholders++;
            }
            console.log(`        [IA] Columnas con placeholder: ${remaining.map(c => c.column_name).join(', ')}`);
        }
        if (!initial.table_description) initial.table_description = 'Descripción pendiente';
        else metrics.tablesAI++;
        return initial;
    } catch (e) {
        console.warn(`      [IA] Fallo generando descripciones para ${database}.${table}: ${e.message}`);
        return { table_description: 'No generado', columns: {} };
    }
}

async function processTable(database, tableName, foreignKeysIndex, pkMap) {
    let columns;
    try {
        columns = await getTableSchema(tableName, database);
    } catch (e) {
        if (e.code === 'ER_TABLEACCESS_DENIED_ERROR' || /access denied/i.test(e.message)) {
            const placeholder = `// Acceso restringido: sin privilegios para leer columnas o datos\nTable ${formatTableName(database, tableName)} {\n  // sin columnas visibles\n}\n\n`;
            noAccessTables.push({ database: database, table: tableName, reason: 'permission denied (columns)' });
            metrics.tablesRestricted++;
            return { dbml: placeholder, description: 'Acceso restringido', table: tableName };
        }
        throw e;
    }
    let sample = [];
    try {
        sample = await getTableSampleData(tableName, database);
    } catch (e) {
        if (e.code === 'ER_TABLEACCESS_DENIED_ERROR' || /access denied/i.test(e.message)) {
            noAccessTables.push({ database: database, table: tableName, reason: 'permission denied (data)' });
            metrics.tablesRestricted++;
        } else {
            console.warn(`Muestra error no crítico ${database}.${tableName}: ${e.message}`);
        }
    }
    let ai = await generateCommentsWithGemini(tableName, database, columns, sample);
    if (!ai.columns) ai.columns = {};
    if (INCLUDE_FK_GUESS) {
        const guesses = guessFkNotes(columns);
        for (const k in guesses) if (!ai.columns[k]) ai.columns[k] = guesses[k];
    }
    let tableStr = '';
    tableStr += `Table ${formatTableName(database, tableName)} {\n`;
    const pkColsArr = pkMap[tableName] || [];
    const compositePk = pkColsArr.length > 1;
    const pkCols = new Set(compositePk ? [] : pkColsArr);
    for (const col of columns) {
        const colNameFmt = isSimpleIdentifier(col.column_name) ? col.column_name : `\`${col.column_name}\``;
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
            const aiDesc = ai.columns[col.column_name];
            if (aiDesc) settings.push(`note: '${escapeNote(aiDesc)}'`);
        }
        if (settings.length) line += ` [${settings.join(', ')}]`;
        tableStr += line + '\n';
    }
    tableStr += '\n';
    if (USE_AI && ai.table_description) {
        tableStr += `  Note: '${escapeNote(ai.table_description)}'\n`;
    }
    tableStr += '}\n\n';
    return { dbml: tableStr, description: ai.table_description || '', table: tableName, columnsMeta: ai.columns, columnsRaw: columns };
}

async function main() {
    const started = Date.now();
    let dbml = '';
    global.noAccessTables = [];
    metrics.startTime = started;
    const emittedRefs = new Set();
    function refKey(srcDb, srcTable, srcColsArr, dstDb, dstTable, dstColsArr) {
        const left = `${srcDb}.${srcTable}.(${srcColsArr.join(',')})`;
        const right = `${dstDb}.${dstTable}.(${dstColsArr.join(',')})`;
        return left + '>' + right;
    }
    console.log('=== CONFIG INICIAL ===');
    console.log(`USE_AI=${USE_AI} MODEL=${USE_AI ? modelName : 'N/A'} CONCURRENCY=${TABLE_CONCURRENCY} SAMPLE_SIZE=${SAMPLE_SIZE}`);
    console.log(`SELECTIVE_RETRY_ATTEMPTS=${SELECTIVE_RETRY_ATTEMPTS} SAVE_EVERY=${SAVE_EVERY}`);
    console.log('======================');
    dbml = `// DBML generado automáticamente para MySQL\n// Fecha: ${new Date().toISOString()}\n\n`;
    try {
        const databases = await getAllDatabases();
        metrics.databases = databases.length;
        const knownTables = new Set();
        const columnsMapGlobal = {};
        for (const db of databases) {
            dbml += `//=============== BASE DE DATOS: ${db} ===============//\n\n`;
            const tables = await getAllTables(db);
            metrics.tablesTotal += tables.length;
            tables.forEach(t => knownTables.add(`${db}.${t}`));
            console.log(`
--- Base de datos ${db} (${tables.length} tablas) ---`);
            const fks = await getForeignKeys(db);
            const uniques = await getUniqueIndexes(db);
            const pkMap = await getPrimaryKeys(db);
            const fkIndex = {};
            for (const fk of fks) {
                const key = `${db}.${fk.table_name}.${fk.column_name}`;
                if (!fkIndex[key]) fkIndex[key] = [];
                fkIndex[key].push(fk);
            }
            const uniqueMap = {};
            for (const u of uniques) {
                if (!uniqueMap[u.table]) uniqueMap[u.table] = [];
                uniqueMap[u.table].push(u.columns);
            }
            let processed = 0;
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
                        console.log(`   [Tabla] Procesando ${db}.${t}`);
                        const meta = await processTable(db, t, fkIndex, pkMap);
                        if ((pkMap[t] && pkMap[t].length > 1) || (uniqueMap[t] && uniqueMap[t].length)) {
                            dbml += meta.dbml.replace(/}\n\n$/, '');
                            dbml += '  indexes {\n';
                            if (pkMap[t] && pkMap[t].length > 1) dbml += `    (${pkMap[t].join(', ')}) [pk]\n`;
                            if (uniqueMap[t]) for (const ucols of uniqueMap[t]) dbml += `    (${ucols.join(', ')}) [unique]\n`;
                            dbml += '  }\n';
                            dbml += '}\n\n';
                        } else {
                            dbml += meta.dbml;
                        }
                        tableMetas.push(meta);
                        const colSet = new Set((meta.columnsRaw || []).map(c => c.column_name));
                        columnsMap[`${db}.${meta.table}`] = colSet;
                        columnsMapGlobal[`${db}.${meta.table}`] = colSet;
                    } catch (e) {
                        console.error(`Error tabla ${db}.${t}: ${e.message}`);
                    }
                    processed++;
                    if (processed % SAVE_EVERY === 0) {
                        fs.writeFileSync(PARTIAL_FILE, dbml);
                        console.log(`Guardado parcial (${processed}/${tables.length} tablas en ${db})`);
                    }
                }
            }
            const workers = Array.from({ length: Math.min(TABLE_CONCURRENCY, tables.length) }, () => worker());
            await Promise.all(workers);
            if (fks.length) {
                if (!/Relaciones explícitas ${db}/.test(dbml)) dbml += `// Relaciones explícitas ${db}\n`;
                const byConstraint = {};
                for (const fk of fks) {
                    if (!byConstraint[fk.constraint_name]) byConstraint[fk.constraint_name] = [];
                    byConstraint[fk.constraint_name].push(fk);
                }
                let countLines = 0;
                for (const cname of Object.keys(byConstraint)) {
                    const rows = byConstraint[cname];
                    if (!rows.length) continue;
                    const srcDb = rows[0].table_schema;
                    const srcTable = rows[0].table_name;
                    const dstDb = rows[0].referenced_table_schema;
                    const dstTable = rows[0].referenced_table_name;
                    if (!knownTables.has(`${srcDb}.${srcTable}`) || !knownTables.has(`${dstDb}.${dstTable}`)) continue;
                    const srcColsSetLocal = columnsMap[`${srcDb}.${srcTable}`] || new Set();
                    const dstColsSetGlobal = columnsMapGlobal[`${dstDb}.${dstTable}`];
                    const orderedPairs = [];
                    const seenPair = new Set();
                    for (const r of rows) {
                        const key = r.column_name + '>' + r.referenced_column_name;
                        if (seenPair.has(key)) continue;
                        seenPair.add(key);
                        orderedPairs.push([r.column_name, r.referenced_column_name]);
                    }
                    const validPairs = orderedPairs.filter(([s, d]) => srcColsSetLocal.has(s) && (!dstColsSetGlobal || dstColsSetGlobal.has(d)));
                    if (!validPairs.length) continue;
                    if (validPairs.length === 1) {
                        const [s, d] = validPairs[0];
                        if (srcDb === dstDb && srcTable === dstTable && s === d) continue;
                        const k = refKey(srcDb, srcTable, [s], dstDb, dstTable, [d]);
                        if (emittedRefs.has(k)) continue;
                        emittedRefs.add(k);
                        dbml += `Ref: ${formatRef(srcDb, srcTable, s)} > ${formatRef(dstDb, dstTable, d)}\n`;
                        countLines++;
                        continue;
                    }
                    const localCols = validPairs.map(p => p[0]);
                    const dstCols = validPairs.map(p => p[1]);
                    if (localCols.length !== dstCols.length) {
                        for (const [s, d] of validPairs.slice(0, Math.min(localCols.length, dstCols.length))) {
                            if (srcDb === dstDb && srcTable === dstTable && s === d) continue;
                            const k = refKey(srcDb, srcTable, [s], dstDb, dstTable, [d]);
                            if (emittedRefs.has(k)) continue;
                            emittedRefs.add(k);
                            dbml += `Ref: ${formatRef(srcDb, srcTable, s)} > ${formatRef(dstDb, dstTable, d)}\n`;
                            countLines++;
                        }
                        continue;
                    }
                    if (srcDb === dstDb && srcTable === dstTable && localCols.length === dstCols.length && localCols.every((c, i) => c === dstCols[i])) {
                        continue;
                    }
                    const compositeKey = refKey(srcDb, srcTable, localCols, dstDb, dstTable, dstCols);
                    if (emittedRefs.has(compositeKey)) continue;
                    emittedRefs.add(compositeKey);
                    const leftCols = localCols.map(c => isSimpleIdentifier(c) ? c : `\`${c}\``).join(', ');
                    const rightCols = dstCols.map(c => isSimpleIdentifier(c) ? c : `\`${c}\``).join(', ');
                    dbml += `Ref: ${isSimpleIdentifier(srcDb) ? srcDb : `\`${srcDb}\``}.${isSimpleIdentifier(srcTable) ? srcTable : `\`${srcTable}\``}.(${leftCols}) > ${isSimpleIdentifier(dstDb) ? dstDb : `\`${dstDb}\``}.${isSimpleIdentifier(dstTable) ? dstTable : `\`${dstTable}\``}.(${rightCols})\n`;
                    countLines++;
                }
                dbml += '\n';
                console.log(`   [FK] ${countLines} relaciones añadidas en ${db} (constraints=${Object.keys(byConstraint).length})`);
            }
            if (!INFER_FKS && INCLUDE_GUESSED_REFS) {
                const existingRefCols = new Set(fks.map(f => `${f.table_name}.${f.column_name}`));
                for (const tm of tableMetas) {
                    if (!tm.columnsRaw) continue;
                    for (const col of tm.columnsRaw) {
                        const cname = col.column_name.toLowerCase();
                        if (existingRefCols.has(`${tm.table}.${col.column_name}`)) continue;
                        if (cname === 'id') continue;
                        let base = null;
                        if (cname.endsWith('_id')) base = cname.slice(0, -3); else if (cname.startsWith('id_')) base = cname.slice(3);
                        if (!base || base.length < 2) continue;
                        const candidates = [base, `${base}s`, base.endsWith('s') ? base.slice(0, -1) : null].filter(Boolean);
                        let targetTable = candidates.find(c => tables.includes(c));
                        if (!targetTable) continue;
                        if (targetTable === tm.table) continue;
                        const k = refKey(db, tm.table, [col.column_name], db, targetTable, ['id']);
                        if (emittedRefs.has(k)) continue;
                        emittedRefs.add(k);
                        const line = `Ref: ${formatRef(db, tm.table, col.column_name)} > ${formatRef(db, targetTable, 'id')} // guessed`;
                        dbml += line + '\n';
                    }
                }
                dbml += '\n';
            }
            if (INFER_FKS) {
                console.log(`   [InferFK] Analizando posibles FKs en base de datos ${db} ...`);
                const existingFkSet = new Set();
                for (const fk of fks) existingFkSet.add(`${fk.table_name}.${fk.column_name}>${fk.referenced_table_name}.${fk.referenced_column_name}`);
                const inferred = await inferForeignKeys(db, tableMetas, pkMap, existingFkSet);
                if (inferred.length) {
                    dbml += `// Relaciones inferidas (heurística valores) ${db}\n`;
                    for (const inf of inferred) {
                        const k = refKey(db, inf.table, [inf.column], db, inf.target_table, [inf.target_column]);
                        if (emittedRefs.has(k)) continue;
                        emittedRefs.add(k);
                        const line = `Ref: ${formatRef(db, inf.table, inf.column)} > ${formatRef(db, inf.target_table, inf.target_column)} // inferred ratio=${inf.ratio.toFixed(2)} matched=${inf.matched}/${inf.total}`;
                        dbml += line + '\n';
                    }
                    dbml += '\n';
                    console.log(`   [InferFK] ${inferred.length} relaciones inferidas en ${db}`);
                } else {
                    console.log(`   [InferFK] 0 relaciones inferidas en ${db}`);
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
        console.log(`Bases de datos: ${metrics.databases}`);
        console.log(`Tablas totales: ${metrics.tablesTotal}`);
        console.log(`Tablas procesadas: ${metrics.tablesProcessed}`);
        console.log(`Tablas con IA: ${metrics.tablesAI}`);
        console.log(`Tablas saltadas: ${metrics.tablesSkipped}`);
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
        await connection.end();
    }
}

if (require.main === module) main();
