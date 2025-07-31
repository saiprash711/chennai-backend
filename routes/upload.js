// routes/upload.js - OPTIMIZED SMART DAILY UPDATE SYSTEM
const express = require('express');
const router = express.Router();
const multer = require('multer');
const xlsx = require('xlsx');
const { pgPool } = require('../config/database');
const { authenticateToken, authorizeRole } = require('../middleware/auth');

// ============================================================================
// PERFORMANCE OPTIMIZATIONS
// ============================================================================

// Optimized multer configuration
const storage = multer.memoryStorage();
const upload = multer({ 
    storage: storage,
    limits: {
        fileSize: 25 * 1024 * 1024, // Reduced to 25MB for better performance
        files: 1,
        fields: 5
    },
    fileFilter: (req, file, cb) => {
        // Only allow specific file types
        const allowedTypes = [
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'text/csv'
        ];
        
        if (allowedTypes.includes(file.mimetype)) {
            cb(null, true);
        } else {
            cb(new Error('Invalid file type. Only XLSX, XLS, and CSV files are allowed.'));
        }
    }
});

// Batch processing configuration
const BATCH_SIZE = 500; // Process records in batches
const MAX_RECORDS = 10000; // Limit total records per upload

// Enhanced date parsing with better performance
const parseDate = (dateString) => {
    if (!dateString) return new Date();
    
    const cleanDate = String(dateString).trim();
    
    // Handle Excel serial dates first (most common)
    const numericDate = parseFloat(cleanDate);
    if (!isNaN(numericDate) && numericDate > 1 && numericDate < 100000) {
        const excelEpoch = new Date(1900, 0, 1);
        return new Date(excelEpoch.getTime() + (numericDate - 2) * 24 * 60 * 60 * 1000);
    }
    
    // Handle DD-MM-YYYY format
    const ddmmyyyyMatch = cleanDate.match(/(\d{1,2})-(\d{1,2})-(\d{4})/);
    if (ddmmyyyyMatch) {
        const [_, day, month, year] = ddmmyyyyMatch;
        return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    }
    
    // Fallback to standard parsing
    const standardDate = new Date(cleanDate);
    return !isNaN(standardDate.getTime()) ? standardDate : new Date();
};

// Create upload history table with optimizations
async function ensureUploadHistoryTable(client) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS upload_history (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            filename VARCHAR(255),
            upload_date TIMESTAMPTZ DEFAULT NOW(),
            records_processed INTEGER DEFAULT 0,
            records_new INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_skipped INTEGER DEFAULT 0,
            date_range_start DATE,
            date_range_end DATE,
            branches_affected TEXT[],
            summary JSONB,
            processing_time_ms INTEGER
        );
        
        CREATE INDEX IF NOT EXISTS idx_upload_history_user_id ON upload_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_upload_history_upload_date ON upload_history(upload_date);
    `);
}

// ============================================================================
// OPTIMIZED UPLOAD ENDPOINT
// ============================================================================

router.post('/', authenticateToken, authorizeRole('smart_user'), upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded.' });
    }

    const startTime = Date.now();
    const client = await pgPool.connect();
    
    try {
        console.log(`🔄 Processing optimized upload: ${req.file.originalname}`);
        
        await client.query('BEGIN');
        await ensureUploadHistoryTable(client);

        // Optimized XLSX reading with better memory management
        const workbook = xlsx.read(req.file.buffer, { 
            type: 'buffer',
            cellDates: true,
            cellNF: false,
            cellText: false,
            sheetStubs: false,
            bookProps: false,
            bookSheets: false,
            bookVBA: false
        });
        
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        // Convert to JSON with better performance
        const data = xlsx.utils.sheet_to_json(worksheet, {
            header: 1,
            defval: '',
            blankrows: false,
            range: 0 // Process all rows, but we'll limit later
        });

        if (!data || data.length < 2) {
            throw new Error("No data found in the uploaded file.");
        }

        // Limit records for performance
        if (data.length > MAX_RECORDS) {
            console.log(`⚠️ File has ${data.length} rows, limiting to ${MAX_RECORDS} for performance`);
            data.splice(MAX_RECORDS + 1);
        }

        // Process headers more efficiently
        const headers = data[0].map(header => String(header).trim());
        const cleanData = data.slice(1).map(row => {
            const obj = {};
            headers.forEach((header, index) => {
                obj[header] = row[index] || '';
            });
            return obj;
        }).filter(row => {
            // Filter out completely empty rows
            return Object.values(row).some(value => value !== '');
        });

        console.log(`📊 Processing ${cleanData.length} data rows for intelligent update...`);

        // Process with optimized batch updates
        const result = await processOptimizedDailyUpdate(cleanData, client, req.user.id, req.file.originalname);

        await client.query('COMMIT');
        
        const processingTime = Date.now() - startTime;
        console.log(`✅ Upload completed in ${processingTime}ms`);
        
        res.status(200).json({ 
            success: true, 
            message: 'Smart daily update completed successfully!',
            processingTime: processingTime,
            ...result
        });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Upload processing error:', error);
        res.status(500).json({ 
            error: 'Failed to process upload.', 
            details: error.message
        });
    } finally {
        client.release();
    }
});

// ============================================================================
// OPTIMIZED DATA PROCESSING
// ============================================================================

async function processOptimizedDailyUpdate(data, client, userId, filename) {
    console.log("🧠 Starting optimized daily data processing...");
    const processingStart = Date.now();

    // Pre-load all reference data in single queries
    const [productRows, branchRows] = await Promise.all([
        client.query('SELECT id, material FROM products'),
        client.query('SELECT id, name FROM branches')
    ]);
    
    const productMap = new Map(productRows.rows.map(p => [p.material.toUpperCase(), p.id]));
    const branchMap = new Map(branchRows.rows.map(b => [b.name.toUpperCase(), b.id]));

    // Process data with optimized validation
    const processedData = new Map();
    const missingProducts = new Set();
    const missingBranches = new Set();
    const dateRange = { start: null, end: null };
    let validRecords = 0;
    let invalidRecords = 0;

    // Process in batches for better memory management
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
        const batch = data.slice(i, i + BATCH_SIZE);
        
        for (const row of batch) {
            try {
                const recordDate = parseDate(row.Date || row.date);
                const formattedDate = recordDate.toISOString().split('T')[0];
                const branchName = (row.Branch || row.branch || '').trim().toUpperCase();
                const itemCode = (row['Item Code'] || row['item code'] || row.itemcode || '').trim().toUpperCase();
                const salesQty = parseFloat(row['Sales Qty.'] || row['sales qty'] || row.salesqty || 0);

                // Track date range
                if (!dateRange.start || recordDate < new Date(dateRange.start)) {
                    dateRange.start = formattedDate;
                }
                if (!dateRange.end || recordDate > new Date(dateRange.end)) {
                    dateRange.end = formattedDate;
                }

                if (!branchName || !itemCode || salesQty <= 0) {
                    invalidRecords++;
                    continue;
                }

                // Track missing entities
                if (!productMap.has(itemCode)) missingProducts.add(itemCode);
                if (!branchMap.has(branchName)) missingBranches.add(branchName);

                const key = `${formattedDate}-${branchName}-${itemCode}`;
                if (!processedData.has(key)) {
                    processedData.set(key, {
                        recordDate: formattedDate,
                        branchName,
                        itemCode,
                        billing: 0,
                        tonnage: parseFloat(row['Tonnage']) || 1.0,
                        star: parseInt(row['Star rating']) || 3,
                        technology: row['Technology'] || 'Non Inv'
                    });
                }
                processedData.get(key).billing += salesQty;
                validRecords++;
                
            } catch (error) {
                console.error('❌ Error processing row:', error.message);
                invalidRecords++;
            }
        }
    }

    console.log(`📊 Data validation complete: ${validRecords} valid, ${invalidRecords} invalid records`);

    // Optimized entity creation with UPSERT
    const createdEntities = { branches: 0, products: 0 };
    
    if (missingBranches.size > 0) {
        const branchValues = Array.from(missingBranches).map((name, index) => 
            `($${index * 4 + 1}, $${index * 4 + 2}, $${index * 4 + 3}, $${index * 4 + 4})`
        ).join(',');
        
        const branchParams = [];
        Array.from(missingBranches).forEach(name => {
            branchParams.push(name, 'Unknown', 15, 70);
        });
        
        if (branchValues) {
            await client.query(`
                INSERT INTO branches (name, state, market_share, penetration) 
                VALUES ${branchValues}
                ON CONFLICT (name) DO NOTHING
            `, branchParams);
            
            // Update our map with new branches
            const newBranches = await client.query(
                'SELECT id, name FROM branches WHERE UPPER(name) = ANY($1)',
                [Array.from(missingBranches)]
            );
            newBranches.rows.forEach(branch => {
                branchMap.set(branch.name.toUpperCase(), branch.id);
            });
            createdEntities.branches = missingBranches.size;
        }
    }

    if (missingProducts.size > 0) {
        const productBatches = [];
        const batchArray = Array.from(missingProducts);
        
        for (let i = 0; i < batchArray.length; i += BATCH_SIZE) {
            const batch = batchArray.slice(i, i + BATCH_SIZE);
            const productValues = batch.map((_, index) => 
                `($${index * 6 + 1}, $${index * 6 + 2}, $${index * 6 + 3}, $${index * 6 + 4}, $${index * 6 + 5}, $${index * 6 + 6})`
            ).join(',');
            
            const productParams = [];
            batch.forEach(itemCode => {
                const productData = Array.from(processedData.values()).find(item => 
                    item.itemCode.toUpperCase() === itemCode
                );
                productParams.push(
                    itemCode,
                    productData?.tonnage || 1.0,
                    productData?.star || 3,
                    productData?.technology || 'Non Inv',
                    35000,
                    0
                );
            });
            
            await client.query(`
                INSERT INTO products (material, tonnage, star, technology, price, factory_stock)
                VALUES ${productValues}
                ON CONFLICT (material) DO NOTHING
            `, productParams);
        }
        
        // Update our map with new products
        const newProducts = await client.query(
            'SELECT id, material FROM products WHERE UPPER(material) = ANY($1)',
            [Array.from(missingProducts)]
        );
        newProducts.rows.forEach(product => {
            productMap.set(product.material.toUpperCase(), product.id);
        });
        createdEntities.products = missingProducts.size;
    }

    // Get existing data for change detection with optimized query
    const existingDataQuery = `
        SELECT 
            i.product_id, i.branch_id, i.record_date, i.billing,
            p.material, b.name as branch_name
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        JOIN branches b ON i.branch_id = b.id
        WHERE i.record_date BETWEEN $1 AND $2
    `;
    
    const existingData = await client.query(existingDataQuery, [dateRange.start, dateRange.end]);
    const existingMap = new Map();
    
    existingData.rows.forEach(row => {
        const key = `${row.record_date.toISOString().split('T')[0]}-${row.branch_name.toUpperCase()}-${row.material.toUpperCase()}`;
        existingMap.set(key, row);
    });

    // Optimized batch insert/update
    let newRecords = 0;
    let updatedRecords = 0;
    let skippedRecords = 0;
    const changes = [];
    const branchesAffected = new Set();

    // Process updates in batches
    const dataEntries = Array.from(processedData.entries());
    
    for (let i = 0; i < dataEntries.length; i += BATCH_SIZE) {
        const batch = dataEntries.slice(i, i + BATCH_SIZE);
        
        // Prepare batch insert/update values
        const insertValues = [];
        const insertParams = [];
        let paramIndex = 1;
        
        for (const [key, value] of batch) {
            const { recordDate, branchName, itemCode, billing } = value;
            const productId = productMap.get(itemCode.toUpperCase());
            const branchId = branchMap.get(branchName.toUpperCase());

            if (productId && branchId) {
                const billingRounded = Math.round(billing);
                const monthPlan = Math.round(billing * 1.2 + 50);
                const avlStock = Math.round(billing * 1.5 + Math.random() * 20);
                const transit = Math.round(billing * 0.3 + Math.random() * 10);
                const opStock = Math.max(0, avlStock + billingRounded - transit);

                branchesAffected.add(branchName);

                // Check for existing record
                const existingRecord = existingMap.get(key);
                
                if (existingRecord) {
                    const significantChange = Math.abs(existingRecord.billing - billingRounded) > 0;
                    
                    if (significantChange) {
                        changes.push({
                            type: 'updated',
                            product: itemCode,
                            branch: branchName,
                            date: recordDate,
                            oldValue: existingRecord.billing,
                            newValue: billingRounded,
                            change: billingRounded - existingRecord.billing
                        });
                        updatedRecords++;
                    } else {
                        skippedRecords++;
                        continue;
                    }
                } else {
                    changes.push({
                        type: 'new',
                        product: itemCode,
                        branch: branchName,
                        date: recordDate,
                        value: billingRounded
                    });
                    newRecords++;
                }

                // Add to batch insert
                insertValues.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7})`);
                insertParams.push(productId, branchId, recordDate, opStock, avlStock, transit, billingRounded, monthPlan);
                paramIndex += 8;
            } else {
                skippedRecords++;
            }
        }

        // Execute batch insert/update
        if (insertValues.length > 0) {
            await client.query(`
                INSERT INTO inventory (product_id, branch_id, record_date, op_stock, avl_stock, transit, billing, month_plan)
                VALUES ${insertValues.join(', ')}
                ON CONFLICT (product_id, branch_id, record_date) DO UPDATE SET
                    op_stock = EXCLUDED.op_stock,
                    avl_stock = EXCLUDED.avl_stock,
                    transit = EXCLUDED.transit,
                    billing = EXCLUDED.billing,
                    month_plan = EXCLUDED.month_plan,
                    updated_at = CURRENT_TIMESTAMP
            `, insertParams);
        }
    }

    // Save upload history with processing time
    const processingTime = Date.now() - processingStart;
    const summary = {
        changes: changes.slice(0, 100),
        createdEntities,
        dateRange,
        processingTimeMs: processingTime,
        topChanges: changes
            .filter(c => c.type === 'updated')
            .sort((a, b) => Math.abs(b.change) - Math.abs(a.change))
            .slice(0, 10)
    };

    // **FIX:** Pass the summary object directly to the query. The 'pg' driver handles JSONB serialization.
    await client.query(`
        INSERT INTO upload_history 
        (user_id, filename, records_processed, records_new, records_updated, records_skipped, 
         date_range_start, date_range_end, branches_affected, summary, processing_time_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, [
        userId, filename, validRecords, newRecords, updatedRecords, skippedRecords,
        dateRange.start, dateRange.end, Array.from(branchesAffected), summary, processingTime
    ]);

    console.log(`✅ Optimized update completed in ${processingTime}ms: ${newRecords} new, ${updatedRecords} updated, ${skippedRecords} skipped`);

    return {
        recordsProcessed: validRecords,
        recordsNew: newRecords,
        recordsUpdated: updatedRecords,
        recordsSkipped: skippedRecords,
        dateRange,
        branchesAffected: Array.from(branchesAffected),
        createdEntities,
        significantChanges: changes.filter(c => c.type === 'updated' && Math.abs(c.change) > 10),
        summary: `Processed ${validRecords} records in ${processingTime}ms: ${newRecords} new, ${updatedRecords} updated, ${skippedRecords} unchanged`,
        processingTimeMs: processingTime
    };
}

// ============================================================================
// OPTIMIZED HISTORY AND FRESHNESS ENDPOINTS
// ============================================================================

router.get('/history', authenticateToken, async (req, res) => {
    try {
        const { limit = 10, offset = 0 } = req.query;
        
        const query = `
            SELECT 
                uh.*,
                u.username as uploaded_by
            FROM upload_history uh
            LEFT JOIN users u ON uh.user_id = u.id
            ORDER BY uh.upload_date DESC
            LIMIT $1 OFFSET $2
        `;
        
        const [historyResult, countResult] = await Promise.all([
            pgPool.query(query, [parseInt(limit), parseInt(offset)]),
            pgPool.query('SELECT COUNT(*) FROM upload_history')
        ]);
        
        res.json({
            success: true,
            history: historyResult.rows,
            total: parseInt(countResult.rows[0].count),
            hasMore: (parseInt(offset) + historyResult.rows.length) < parseInt(countResult.rows[0].count)
        });
        
    } catch (error) {
        console.error('❌ Error fetching upload history:', error);
        res.status(500).json({ 
            error: 'Failed to fetch upload history',
            details: error.message
        });
    }
});

router.get('/freshness', authenticateToken, async (req, res) => {
    try {
        // **FIX:** Changed `CURRENT_DATE - MAX(i.record_date)` to `EXTRACT(DAY FROM ...)` to return an integer instead of an interval.
        const query = `
            SELECT 
                b.name as branch_name,
                MAX(i.record_date) as latest_data_date,
                COUNT(DISTINCT i.record_date) as days_of_data,
                MAX(i.updated_at) as last_updated,
                EXTRACT(DAY FROM (CURRENT_DATE - MAX(i.record_date))) as days_old,
                COUNT(i.id) as total_records
            FROM branches b
            LEFT JOIN inventory i ON b.id = i.branch_id
            GROUP BY b.id, b.name
            ORDER BY days_old ASC NULLS LAST
        `;
        
        const result = await pgPool.query(query);
        const freshness = result.rows;
        
        // Calculate overview stats
        const overview = {
            totalBranches: freshness.length,
            upToDate: freshness.filter(r => r.days_old <= 1).length,
            needsUpdate: freshness.filter(r => r.days_old > 3).length,
            totalRecords: freshness.reduce((sum, r) => sum + parseInt(r.total_records || 0), 0)
        };
        
        res.json({ success: true, freshness, overview });
        
    } catch (error) {
        console.error('❌ Error fetching data freshness:', error);
        res.status(500).json({ 
            error: 'Failed to fetch data freshness',
            details: error.message
        });
    }
});

module.exports = router;
