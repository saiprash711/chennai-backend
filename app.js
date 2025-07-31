const express = require('express');
const cors =require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0'; // ADDED: Render requirement

// --- Helper function to validate imported routes ---
function validateRouter(routerModule, filePath) {
  if (typeof routerModule !== 'function' || !routerModule.stack) {
    throw new Error(
      `❌ FATAL: Failed to load router from '${filePath}'.\n` +
      `This is not a valid Express router. Please check the file for syntax errors or ensure 'module.exports = router;' is correct.`
    );
  }
  return routerModule;
}

// --- SECURITY ENHANCEMENTS ---
const apiLimiter = rateLimit({
	windowMs: 15 * 60 * 1000,
	max: 100,
	standardHeaders: true,
	legacyHeaders: false,
    message: { error: 'Too many requests from this IP, please try again after 15 minutes.' },
});

const blockScrapers = (req, res, next) => {
    const userAgent = req.get('User-Agent');
    if (userAgent) {
        const blockedAgents = ['python-requests', 'scrapy', 'node-fetch', 'wget', 'curl', 'postman'];
        if (blockedAgents.some(agent => userAgent.toLowerCase().includes(agent))) {
            return res.status(404).json({ error: 'Endpoint not found' });
        }
    }
    next();
};

// --- MIDDLEWARE ---
app.use(helmet());

// UPDATED: Enhanced CORS configuration for Render
app.use(cors({
  origin: [
    'http://localhost:3000',
    'https://chennai-frontend.vercel.app',
    /\.vercel\.app$/,
    /\.netlify\.app$/,
    /localhost:\d{4}$/
  ],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(morgan('combined'));

app.use('/api/', apiLimiter);
app.use('/api/', blockScrapers);

// ADDED: Root health check for Render
app.get('/', (req, res) => {
  res.status(200).json({ 
    message: 'Hansei Backend API is running!',
    status: 'healthy',
    timestamp: new Date().toISOString(),
    version: '1.0.1'
  });
});

// --- Import and Validate Routes ---
try {
    const authRoutes = validateRouter(require('./routes/auth'), './routes/auth.js');
    const salesRoutes = validateRouter(require('./routes/sales'), './routes/sales.js');
    const analyticsRoutes = validateRouter(require('./routes/analytics'), './routes/analytics.js');
    const uploadRoutes = validateRouter(require('./routes/upload'), './routes/upload.js');
    const chatbotRoutes = validateRouter(require('./routes/chatbot'), './routes/chatbot.js');

    // --- Use Routes ---
    app.use('/api/auth', authRoutes);
    app.use('/api/sales', salesRoutes);
    app.use('/api/analytics', analyticsRoutes);
    app.use('/api/upload', uploadRoutes);
    app.use('/api/chatbot', chatbotRoutes);

} catch (error) {
    console.error(error.message);
    process.exit(1);
}

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.status(200).json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    version: '1.0.1',
    database: 'connected'
  });
});

// ADDED: Enhanced error handling for Render
app.use((err, req, res, next) => {
  console.error('Error Stack:', err.stack);
  console.error('Error Message:', err.message);
  
  // Don't leak error details in production
  const isDevelopment = process.env.NODE_ENV === 'development';
  
  res.status(err.status || 500).json({
    error: {
      message: isDevelopment ? err.message : 'Internal server error',
      status: err.status || 500,
      ...(isDevelopment && { stack: err.stack })
    }
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: {
      message: 'Endpoint not found',
      status: 404,
      path: req.path
    }
  });
});

// UPDATED: Enhanced server startup for Render
const server = app.listen(PORT, HOST, () => {
  console.log(`🚀 Hansei Backend Server running on ${HOST}:${PORT}`);
  console.log(`📊 Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`🌐 Health check: http://${HOST}:${PORT}/api/health`);
  console.log('🔒 Anti-scraping measures are active.');
});

// ADDED: Graceful shutdown handling
process.on('SIGTERM', () => {
  console.log('SIGTERM received');
  if (server) {
    server.close(() => {
      console.log('HTTP server closed');
      process.exit(0);
    });
  }
});

process.on('SIGINT', () => {
  console.log('SIGINT received');
  if (server) {
    server.close(() => {
      console.log('HTTP server closed');
      process.exit(0);
    });
  }
});

module.exports = app;
