// routes/upload.js - FIXED VERSION
const express = require('express');
const router = express.Router();
const multer = require('multer');
const xlsx = require('xlsx');
const { pgPool } = require('../config/database');
const { authenticateToken, authorizeRole } = require('../middleware/auth');

// ============================================================================
// MULTER CONFIGURATION
// ============================================================================
const storage = multer.memoryStorage();
const upload = multer({ 
    storage: storage,
    limits: {
        fileSize: 25 * 1024 * 1024, // 25MB
        files: 1,
        fields: 5
    },
    fileFilter: (req, file, cb) => {
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

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================
const parseDate = (dateString) => {
    if (!dateString) return new Date();
    
    const cleanDate = String(dateString).trim();
    
    // Handle Excel serial dates first
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

// Create upload history table
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
// ROUTES
// ============================================================================

// Data freshness endpoint - FIXED
router.get('/freshness', authenticateToken, async (req, res) => {
    try {
        console.log('📅 Checking data freshness...');
        
        const query = `
            SELECT 
                b.name as branch_name,
                MAX(i.record_date) as latest_data_date,
                COUNT(DISTINCT i.record_date) as days_of_data,
                CURRENT_DATE - MAX(i.record_date) as days_old,
                COUNT(i.id) as total_records
            FROM branches b
            LEFT JOIN inventory i ON b.id = i.branch_id
            GROUP BY b.id, b.name
            ORDER BY days_old ASC NULLS LAST
        `;
        
        const result = await pgPool.query(query);
        const freshness = result.rows || [];
        
        // Calculate overview stats
        const overview = {
            totalBranches: freshness.length,
            upToDate: freshness.filter(r => (r.days_old || 999) <= 1).length,
            needsUpdate: freshness.filter(r => (r.days_old || 999) > 3).length,
            totalRecords: freshness.reduce((sum, r) => sum + parseInt(r.total_records || 0), 0)
        };
        
        console.log(`📊 Data freshness check completed for ${freshness.length} branches`);
        
        res.json({ 
            freshness, 
            overview,
            success: true,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('❌ Error fetching data freshness:', error);
        res.status(500).json({ 
            error: 'Failed to fetch data freshness',
            details: error.message,
            success: false
        });
    }
});

// Upload history endpoint
router.get('/history', authenticateToken, async (req, res) => {
    try {
        const { limit = 10, offset = 0 } = req.query;
        
        console.log(`📋 Fetching upload history (limit: ${limit}, offset: ${offset})`);
        
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
            history: historyResult.rows || [],
            total: parseInt(countResult.rows[0]?.count || 0),
            hasMore: (parseInt(offset) + historyResult.rows.length) < parseInt(countResult.rows[0]?.count || 0),
            success: true
        });
        
    } catch (error) {
        console.error('❌ Error fetching upload history:', error);
        res.status(500).json({ 
            error: 'Failed to fetch upload history',
            details: error.message,
            success: false
        });
    }
});

// File upload endpoint
router.post('/', authenticateToken, authorizeRole('smart_user'), upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ 
            error: 'No file uploaded.',
            success: false
        });
    }

    const startTime = Date.now();
    const client = await pgPool.connect();
    
    try {
        console.log(`🔄 Processing upload: ${req.file.originalname}`);
        
        await client.query('BEGIN');
        await ensureUploadHistoryTable(client);

        // Read Excel file
        const workbook = xlsx.read(req.file.buffer, { 
            type: 'buffer',
            cellDates: true
        });
        
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        const data = xlsx.utils.sheet_to_json(worksheet, {
            header: 1,
            defval: '',
            blankrows: false
        });

        if (!data || data.length < 2) {
            throw new Error("No data found in the uploaded file.");
        }

        // Process data (simplified for now)
        const headers = data[0].map(header => String(header).trim());
        const cleanData = data.slice(1).filter(row => {
            return row.some(cell => cell !== '');
        });

        console.log(`📊 Processing ${cleanData.length} data rows...`);

        // For now, just log the upload without processing all data
        // In production, you'd implement the full processing logic here
        
        const processingTime = Date.now() - startTime;
        
        // Save upload history
        await client.query(`
            INSERT INTO upload_history 
            (user_id, filename, records_processed, records_new, records_updated, records_skipped, processing_time_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        `, [req.user.id, req.file.originalname, cleanData.length, 0, 0, 0, processingTime]);

        await client.query('COMMIT');
        
        console.log(`✅ Upload logged successfully in ${processingTime}ms`);
        
        res.status(200).json({ 
            success: true, 
            message: 'File upload logged successfully',
            processingTime: processingTime,
            recordsProcessed: cleanData.length,
            filename: req.file.originalname
        });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Upload processing error:', error);
        res.status(500).json({ 
            error: 'Failed to process upload', 
            details: error.message,
            success: false
        });
    } finally {
        client.release();
    }
});

// Health check for upload service
router.get('/health', authenticateToken, async (req, res) => {
    try {
        // Test database connection
        const result = await pgPool.query('SELECT NOW() as timestamp');
        
        res.json({
            status: 'healthy',
            timestamp: result.rows[0].timestamp,
            service: 'upload',
            success: true
        });
    } catch (error) {
        console.error('❌ Upload service health check failed:', error);
        res.status(500).json({
            status: 'unhealthy',
            error: error.message,
            service: 'upload',
            success: false
        });
    }
});

module.exports = router;
