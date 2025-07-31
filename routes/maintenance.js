// routes/maintenance.js - Data Maintenance Routes

const express = require('express');
const router = express.Router();
const { pgPool } = require('../config/database');
const { authenticateToken } = require('../middleware/auth');

// Maintenance settings - configurable
const MAINTENANCE_CONFIG = {
  retentionDays: 365, // Keep data for 1 year
  lowStockThreshold: 5, // Alert if stock < 5
  planAchievementThreshold: 80 // % for performance alerts
};

// Get maintenance dashboard
router.get('/dashboard', authenticateToken, async (req, res) => {
  try {
    const client = await pgPool.connect();
    try {
      // Get system stats
      const stats = await client.query(`
        SELECT 
          COUNT(*) as total_records,
          COUNT(DISTINCT branch_id) as branches,
          COUNT(DISTINCT product_id) as products,
          MIN(record_date) as oldest_data,
          MAX(record_date) as newest_data
        FROM inventory
      `);

      // Get quality metrics
      const quality = await client.query(`
        SELECT 
          SUM(CASE WHEN avl_stock < 0 THEN 1 ELSE 0 END) as negative_stock,
          SUM(CASE WHEN billing < 0 THEN 1 ELSE 0 END) as negative_billing,
          SUM(CASE WHEN month_plan = 0 AND billing > 0 THEN 1 ELSE 0 END) as unplanned_sales
        FROM inventory
      `);

      res.json({
        stats: stats.rows[0],
        quality: quality.rows[0],
        config: MAINTENANCE_CONFIG
      });
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Maintenance dashboard error:', error);
    res.status(500).json({ error: 'Failed to get dashboard data' });
  }
});

// Run manual cleanup
router.post('/cleanup', authenticateToken, async (req, res) => {
  try {
    const client = await pgPool.connect();
    try {
      await client.query('BEGIN');
      
      // Clean old data
      const deleted = await client.query(`
        DELETE FROM inventory 
        WHERE record_date < CURRENT_DATE - INTERVAL '${MAINTENANCE_CONFIG.retentionDays} days'
      `);
      
      await client.query('COMMIT');
      
      res.json({
        success: true,
        deletedRecords: deleted.rowCount
      });
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error('Cleanup error:', error);
    res.status(500).json({ error: 'Cleanup failed' });
  }
});

module.exports = router;