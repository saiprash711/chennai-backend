// ============================================================================
// HANSEI BACKEND - PRODUCTION READY
// ============================================================================
const express = require('express');
const cors = require('cors'); // Make sure to use the cors package
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// ============================================================================
// SECURE CORS CONFIGURATION (FIXED)
// ============================================================================
// Define the list of primary allowed origins
const allowedOrigins = [
  'http://localhost',
  'http://127.0.0.1',
  'https://chennai-frontend.vercel.app' // Your main production frontend
];

// Configure CORS options
const corsOptions = {
  origin: function (origin, callback) {
    // Allow requests with no origin (like mobile apps or curl requests)
    if (!origin) return callback(null, true);

    // Check if the origin is in our exact list (handles localhost with any port)
    if (allowedOrigins.some(allowed => origin.startsWith(allowed))) {
      return callback(null, true);
    }

    // Allow any Vercel preview deployments for your projects
    if (/\.vercel\.app$/.test(origin)) {
        return callback(null, true);
    }

    console.error(`CORS Error: Origin ${origin} not allowed.`);
    callback(new Error('Not allowed by CORS'));
  },
  credentials: true,
  optionsSuccessStatus: 200,
  // FIX: Explicitly allow the 'X-Requested-With' header sent by the browser
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
};

// Use the CORS middleware with your options. This MUST come before your routes.
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

app.get('/api/health', (req, res) => {
    res.json({ 
        status: 'healthy',
        cors: 'WORKING',
        timestamp: new Date().toISOString(),
        origin: req.get('Origin') || 'null'
    });
});

// ============================================================================
// LOAD YOUR EXISTING ROUTES (with error handling)
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

    console.log('✅ Core routes (auth, sales, analytics, upload, chatbot) loaded');

} catch (error) {
    console.log('❌ CRITICAL ERROR: Could not load core routes. Server may not function correctly.', error);
}

// ============================================================================
// GLOBAL ERROR HANDLING
// ============================================================================
app.use((err, req, res, next) => {
    console.error('❌ Global Error Handler Caught:', err.stack);
    res.status(500).json({ error: 'An internal server error occurred.' });
});

app.use((req, res) => {
    res.status(404).json({ error: 'Endpoint not found', path: req.path });
});

// ============================================================================
// START SERVER WITH GRACEFUL ERROR HANDLING
// ============================================================================
const server = app.listen(PORT, () => {
    console.log('🚀 ==========================================');
    console.log('🚀 HANSEI BACKEND STARTED SUCCESSFULLY!');
    console.log('🚀 ==========================================');
    console.log(`📍 URL: http://localhost:${PORT}`);
    console.log(`🔥 CORS: SECURELY ENABLED`);
    console.log('🚀 ==========================================');
    
    // Test database connection after server starts
    setTimeout(async () => {
        try {
            // Assuming database.js exports this function
            const { testDatabaseConnections } = require('./config/database');
            await testDatabaseConnections();
        } catch (error) {
            console.warn('⚠️ Database connection failed on startup check:', error.message);
            console.warn('ℹ️ Server will continue, but database features may not work.');
        }
    }, 1000);
});

server.on('error', (error) => {
    if (error.syscall !== 'listen') {
        throw error;
    }

    const bind = typeof PORT === 'string' ? 'Pipe ' + PORT : 'Port ' + PORT;

    switch (error.code) {
        case 'EACCES':
            console.error(`❌ ${bind} requires elevated privileges.`);
            process.exit(1);
            break;
        case 'EADDRINUSE':
            console.error(`❌ ${bind} is already in use.`);
            console.error('Please stop the other process running on this port or change the PORT in your .env file.');
            process.exit(1);
            break;
        default:
            throw error;
    }
});

console.log('🔄 Starting Hansei Backend Server...');
