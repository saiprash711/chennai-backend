// ============================================================================
// ENHANCED DATABASE CONFIGURATION - PRODUCTION READY v2.2.0 - FIXED FOR RENDER
// ============================================================================
const { Pool } = require('pg');
const mongoose = require('mongoose');
require('dotenv').config();
const url = require('url'); // For parsing DATABASE_URL

// ============================================================================
// POSTGRESQL CONNECTION POOL - NOW HANDLES DATABASE_URL
// ============================================================================
let pgConfig = {
  max: process.env.NODE_ENV === 'production' ? 30 : 10,
  min: 2,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 30000,
  query_timeout: 30000,
  allowExitOnIdle: false,
  application_name: 'hansei_backend_v220'
};

// Parse DATABASE_URL if present (for Render.com)
if (process.env.DATABASE_URL) {
  const params = url.parse(process.env.DATABASE_URL);
  const auth = params.auth.split(':');
  
  pgConfig.user = auth[0];
  pgConfig.password = auth[1];
  pgConfig.host = params.hostname;
  pgConfig.port = params.port;
  pgConfig.database = params.pathname.split('/')[1];
  pgConfig.ssl = { rejectUnauthorized: false };
} else {
  // Fallback to individual env vars
  pgConfig.host = process.env.DB_HOST || 'localhost';
  pgConfig.port = process.env.DB_PORT || 5432;
  pgConfig.database = process.env.DB_NAME || 'hansei_dashboard';
  pgConfig.user = process.env.DB_USER || 'postgres';
  pgConfig.password = process.env.DB_PASSWORD || '';
  pgConfig.ssl = process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : undefined;
}

const pgPool = new Pool(pgConfig);

// Connection monitoring
pgPool.on('connect', (client) => {
  console.log('✅ PostgreSQL connected');
  client.query(`SET work_mem='4MB'; SET random_page_cost=1.1;`)
    .catch((err) => console.warn('⚠️ PG session settings failed:', err.message));
});

pgPool.on('error', (err) => {
  console.error('❌ PostgreSQL pool error:', err.message);
});

// ============================================================================
// MONGODB SCHEMAS (for chatbot and analytics logging)
// ============================================================================
let ChatLog = null;
let AnalyticsEvent = null;

const connectMongoDB = async () => {
  if (!process.env.MONGODB_URI) {
    console.log('ℹ️ MongoDB URI not provided - creating mock schemas');
    
    // Create mock schemas that won't crash the application
    ChatLog = {
      save: async function() {
        console.log('Mock ChatLog save called (MongoDB not connected)');
        return this;
      }
    };
    
    AnalyticsEvent = function(data) {
      return {
        save: async function() {
          console.log('Mock AnalyticsEvent save called (MongoDB not connected)');
          return this;
        }
      };
    };
    
    return;
  }

  try {
    await mongoose.connect(process.env.MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      serverSelectionTimeoutMS: 10000,
      socketTimeoutMS: 45000,
      maxPoolSize: 20,
      minPoolSize: 2,
      autoIndex: false
    });
    
    console.log('✅ MongoDB connected');
    
    // Define schemas after connection
    const chatLogSchema = new mongoose.Schema({
      userId: { type: Number, required: true },
      message: { type: String, required: true },
      response: { type: String, required: true },
      sessionId: { type: String, default: '' },
      timestamp: { type: Date, default: Date.now }
    });

    const analyticsEventSchema = new mongoose.Schema({
      eventType: { type: String, required: true },
      userId: Number,
      metadata: mongoose.Schema.Types.Mixed,
      timestamp: { type: Date, default: Date.now }
    });

    ChatLog = mongoose.model('ChatLog', chatLogSchema);
    AnalyticsEvent = mongoose.model('AnalyticsEvent', analyticsEventSchema);
    
  } catch (err) {
    console.error('❌ MongoDB connection failed:', err.message);
    console.warn('⚠️ Creating mock schemas for graceful degradation');
    
    // Create mock schemas for graceful degradation
    ChatLog = function(data) {
      this.userId = data.userId;
      this.message = data.message;
      this.response = data.response;
      this.sessionId = data.sessionId;
      this.timestamp = new Date();
      
      this.save = async function() {
        console.log('Mock ChatLog save (MongoDB failed):', this.message.substring(0, 50));
        return this;
      };
      return this;
    };
    
    ChatLog.find = function() {
      return {
        sort: function() {
          return {
            limit: function() {
              return Promise.resolve([]);
            }
          };
        }
      };
    };
    
    AnalyticsEvent = function(data) {
      this.eventType = data.eventType;
      this.userId = data.userId;
      this.timestamp = data.timestamp || new Date();
      
      this.save = async function() {
        console.log('Mock AnalyticsEvent save (MongoDB failed):', this.eventType);
        return this;
      };
      return this;
    };
  }
};

// Health checks
async function testDatabaseConnections() {
  // Postgres
  try {
    const client = await pgPool.connect();
    const { rows } = await client.query('SELECT NOW() as now');
    console.log('⏱️ Postgres time:', rows[0].now);
    client.release();
  } catch (err) {
    console.error('❌ PostgreSQL test failed:', err.message);
    throw err;
  }
}

async function initializeDatabase() {
  await connectMongoDB();
  await testDatabaseConnections();
  console.log('🎉 Database init complete');
}

// Graceful shutdown
async function shutdownDatabases() {
  console.log('🔄 Closing database connections');
  await pgPool.end();
  if (mongoose.connection.readyState === 1) {
    await mongoose.connection.close();
  }
}

process.on('SIGINT', shutdownDatabases);
process.on('SIGTERM', shutdownDatabases);

module.exports = {
  pgPool,
  ChatLog,
  AnalyticsEvent,
  initializeDatabase,
  testDatabaseConnections
};
