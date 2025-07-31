// ============================================================================
// HANSEI BACKEND - PRODUCTION READY
// ============================================================================
const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// ============================================================================
// SECURE CORS CONFIGURATION
// ============================================================================
const allowedOrigins = [
  'http://localhost',
  'http://127.0.0.1',
  'null',
  'https://chennai-frontend.vercel.app'
];

const corsOptions = {
  origin: function (origin, callback) {
    const isAllowed = allowedOrigins.some(allowedOrigin => {
      if (allowedOrigin === 'null' && !origin) return true;
      return origin && origin.startsWith(allowedOrigin);
    });

    if (isAllowed || !origin) {
      callback(null, true);
    } else {
      console.error(`CORS Error: Origin ${origin} not allowed.`);
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  optionsSuccessStatus: 200
};

app.use(cors(corsOptions));

// ============================================================================
// BASIC MIDDLEWARE
// ============================================================================
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ============================================================================
// TEST ROUTES
// ============================================================================
app.get('/', (req, res) => {
  res.json({ 
    message: 'Hansei Backend is WORKING!',
    cors: 'ENABLED - Secure Configuration',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/health', async (req, res) => {
  try {
    // Test PG connection
    const pgResult = await pgPool.query('SELECT NOW() as pg_time');
    const pgStatus = 'healthy';
    
    res.json({ 
      status: 'healthy',
      cors: 'WORKING',
      timestamp: new Date().toISOString(),
      origin: req.get('Origin') || 'null',
      database: {
        postgres: pgStatus,
        pgTime: pgResult.rows[0].pg_time
      }
    });
  } catch (error) {
    console.error('Health check failed:', error);
    res.status(503).json({
      status: 'unhealthy',
      error: 'Database connection failed'
    });
  }
});

// ============================================================================
// LOAD ROUTES WITH ERROR HANDLING
// ============================================================================
try {
  const authRoutes = require('./routes/auth');
  const salesRoutes = require('./routes/sales');
  const analyticsRoutes = require('./routes/analytics');
  const uploadRoutes = require('./routes/upload');
  const chatbotRoutes = require('./routes/chatbot');
  
  app.use('/api/auth', authRoutes);
  app.use('/api/sales', salesRoutes);
  app.use('/api/analytics', analyticsRoutes);
  app.use('/api/upload', uploadRoutes);
  app.use('/api/chatbot', chatbotRoutes);

  console.log('✅ Core routes loaded');

} catch (error) {
  console.error('❌ Failed to load routes:', error);
}

// ============================================================================
// GLOBAL ERROR HANDLING
// ============================================================================
app.use((err, req, res, next) => {
  console.error('❌ Global Error:', err.stack);
  res.status(500).json({ error: 'Internal server error.' });
});

app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found', path: req.path });
});

// ============================================================================
// START SERVER
// ============================================================================
const server = app.listen(PORT, () => {
  console.log(`🚀 Server started on port ${PORT}`);
});

// Handle startup errors
server.on('error', (error) => {
  if (error.syscall !== 'listen') throw error;
  
  const bind = typeof PORT === 'string' ? 'Pipe ' + PORT : 'Port ' + PORT;
  
  switch (error.code) {
    case 'EACCES':
      console.error(`❌ ${bind} requires elevated privileges.`);
      process.exit(1);
    case 'EADDRINUSE':
      console.error(`❌ ${bind} is already in use.`);
      process.exit(1);
    default:
      throw error;
  }
});
