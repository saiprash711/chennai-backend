// scripts/import-excel.js - FIXED VERSION WITH BETTER DATE PARSING

const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const { pgPool } = require('../config/database');
const bcrypt = require('bcryptjs');

const parseDate = (dateString) => {
  if (!dateString) return new Date().toISOString().split('T')[0];
  
  const clean = String(dateString).trim();
  
  // DD-MM-YYYY HH:MM
  const ddmmMatch = clean.match(/(\d{1,2})-(\d{1,2})-(\d{4})/);
  if (ddmmMatch) {
    return `${ddmmMatch[3]}-${ddmmMatch[2].padStart(2, '0')}-${ddmmMatch[1].padStart(2, '0')}`;
  }
  
  // Excel serial
  const num = parseFloat(clean);
  if (!isNaN(num) && num > 1) {
    const date = new Date(1900, 0, num - 1);
    return date.toISOString().split('T')[0];
  }
  
  // Standard
  const stdDate = new Date(clean);
  if (!isNaN(stdDate.getTime())) {
    return stdDate.toISOString().split('T')[0];
  }
  
  return new Date().toISOString().split('T')[0];
};

async function importData() {
  const client = await pgPool.connect();
  try {
    const filePath = path.join(__dirname, '../Book1.csv');
    if (!fs.existsSync(filePath)) throw new Error(`CSV not found: ${filePath}`);

    const data = await new Promise((resolve, reject) => {
      const results = [];
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => results.push(row))
        .on('end', () => resolve(results))
        .on('error', reject);
    });

    await client.query('BEGIN');

    // Create users if not exist
    const hashedPass1 = await bcrypt.hash('Daikin@Hansei2025!', 10);
    const hashedPass2 = await bcrypt.hash('Analytics@2025', 10);
    await client.query(`
      INSERT INTO users (username, password, full_name, role) 
      VALUES 
        ('smart', $1, 'Smart User', 'smart_user'),
        ('normal', $2, 'Normal User', 'normal_user')
      ON CONFLICT (username) DO NOTHING
    `, [hashedPass1, hashedPass2]);

    // Process branches
    const branches = [...new Set(data.map(row => row.Branch?.trim()))].filter(Boolean);
    for (const branch of branches) {
      await client.query(`
        INSERT INTO branches (name, state, market_share, penetration) 
        VALUES ($1, 'Tamil Nadu', 15, 70)
        ON CONFLICT (name) DO NOTHING
      `, [branch]);
    }

    // Process products
    const products = [...new Set(data.map(row => row['Item Code']?.trim().toUpperCase()))].filter(Boolean);
    for (const code of products) {
      const row = data.find(r => r['Item Code']?.trim().toUpperCase() === code);
      const tonnage = parseFloat(row.Tonnage) || 1.5;
      const star = parseInt(row['Star rating']) || 3;
      const tech = row.Technology?.includes('Inv') ? 'Inv' : 'Non Inv';
      const price = 30000 + (tonnage * 10000) + (star * 5000);

      await client.query(`
        INSERT INTO products (material, tonnage, star, technology, price, factory_stock)
        VALUES ($1, $2, $3, $4, $5, 0)
        ON CONFLICT (material) DO NOTHING
      `, [code, tonnage, star, tech, price]);
    }

    // Get maps
    const [prodMapRes, branchMapRes] = await Promise.all([
      client.query('SELECT id, material FROM products'),
      client.query('SELECT id, name FROM branches')
    ]);
    const prodMap = new Map(prodMapRes.rows.map(r => [r.material.toUpperCase(), r.id]));
    const branchMap = new Map(branchMapRes.rows.map(r => [r.name.toUpperCase(), r.id]));

    // Process inventory
    const inventoryMap = new Map();
    for (const row of data) {
      const date = parseDate(row.Date);
      const branch = row.Branch?.trim().toUpperCase();
      const code = row['Item Code']?.trim().toUpperCase();
      const billing = parseFloat(row['Sales Qty.']) || 0;

      if (branch && code && billing > 0) {
        const key = `${date}-${branch}-${code}`;
        inventoryMap.set(key, (inventoryMap.get(key) || 0) + billing);
      }
    }

    for (const [key, billing] of inventoryMap) {
      const [date, branch, code] = key.split('-');
      const branchId = branchMap.get(branch);
      const prodId = prodMap.get(code);
      
      if (branchId && prodId) {
        const monthPlan = Math.round(billing * 1.2);
        const avlStock = Math.round(billing * 0.8);
        const transit = Math.round(billing * 0.3);
        const opStock = avlStock + transit;

        await client.query(`
          INSERT INTO inventory (product_id, branch_id, record_date, op_stock, avl_stock, transit, billing, month_plan)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          ON CONFLICT (product_id, branch_id, record_date) DO UPDATE SET
            op_stock = EXCLUDED.op_stock,
            avl_stock = EXCLUDED.avl_stock,
            transit = EXCLUDED.transit,
            billing = EXCLUDED.billing,
            month_plan = EXCLUDED.month_plan
        `, [prodId, branchId, date, opStock, avlStock, transit, Math.round(billing), monthPlan]);
      }
    }

    await client.query('COMMIT');
    console.log('✅ Import complete');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

if (require.main === module) {
  importData().catch(err => {
    console.error('Import failed:', err);
    process.exit(1);
  });
}

module.exports = { importData };
