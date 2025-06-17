// config.js - Configuration file for MongoDB Atlas Fair Marketplace Demo
// Store all sensitive information and configurable settings here

const config = {
    // MongoDB Connection Settings
    database: {
        // Connection string - set via environment variable or update directly
        connectionString: process.env.MONGODB_URI || 'mongodb+srv://username:password@cluster.mongodb.net/',
        
        // Database and Collection Names
        databaseName: process.env.DB_NAME || 'marketplace_demo',
        collectionName: process.env.COLLECTION_NAME || 'offers',
        
        // Connection Options
        options: {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            maxPoolSize: 10,
            serverSelectionTimeoutMS: 5000,
            socketTimeoutMS: 45000,
        }
    },

    // Application Settings
    app: {
        // Default pagination settings
        defaultPageSize: 10,
        maxPageSize: 100,
        
        // Round-robin settings
        roundRobin: {
            seedUpdateIntervalMinutes: 1, // How often the random seed changes
            maxSellersPerPage: 50
        },
        
        // Quota-based distribution settings
        quota: {
            defaultMaxOffersPerSeller: 2,
            maxMaxOffersPerSeller: 10
        },
        
        // Sample data generation settings
        seeding: {
            defaultOfferCount: 100,
            defaultSellerCount: 15,
            gameCategories: ['RPG', 'FPS', 'Strategy', 'Sports', 'Racing', 'Adventure', 'Simulation', 'Puzzle'],
            priceRange: { min: 10, max: 100 },
            stockRange: { min: 1, max: 50 },
            dataRetentionDays: 30
        }
    },

    // Performance and Indexing
    performance: {
        // Index definitions
        indexes: [
            { key: { gameCategory: 1, sellerId: 1 }, name: 'category_seller_idx' },
            { key: { gameCategory: 1, createdAt: -1 }, name: 'category_date_idx' },
            { key: { sellerId: 1 }, name: 'seller_idx' },
            { key: { price: 1 }, name: 'price_idx' },
            { key: { rating: -1 }, name: 'rating_idx' },
            { key: { gameCategory: 1, sellerId: 1, createdAt: -1 }, name: 'compound_roundrobin_idx' },
            { key: { createdAt: -1 }, name: 'date_idx' }
        ],
        
        // Aggregation pipeline settings
        aggregation: {
            allowDiskUse: true,
            maxTimeMS: 30000
        }
    },

    // Environment-specific settings
    environment: {
        isDevelopment: process.env.NODE_ENV !== 'production',
        logLevel: process.env.LOG_LEVEL || 'info',
        enableDebugOutput: process.env.DEBUG === 'true'
    },

    // CLI Display Settings
    display: {
        resultsTableWidth: 80,
        maxTitleLength: 25,
        maxSellerNameLength: 12,
        priceDisplayWidth: 6
    }
};

// Validation function to check required configurations
function validateConfig() {
    const errors = [];

    // Check database connection string
    if (!config.database.connectionString || config.database.connectionString.includes('username:password')) {
        errors.push('MongoDB connection string is not properly configured. Please set MONGODB_URI environment variable.');
    }

    // Check database name
    if (!config.database.databaseName) {
        errors.push('Database name is required.');
    }

    // Check collection name
    if (!config.database.collectionName) {
        errors.push('Collection name is required.');
    }

    // Validate pagination settings
    if (config.app.defaultPageSize > config.app.maxPageSize) {
        errors.push('Default page size cannot be larger than maximum page size.');
    }

    return errors;
}

// Helper function to get environment-specific database name
function getDatabaseName() {
    const env = process.env.NODE_ENV || 'development';
    if (env === 'test') {
        return `${config.database.databaseName}_test`;
    }
    return config.database.databaseName;
}

// Helper function to get full collection reference
function getCollectionReference() {
    return `${getDatabaseName()}.${config.database.collectionName}`;
}

// Export configuration and helper functions
module.exports = {
    config,
    validateConfig,
    getDatabaseName,
    getCollectionReference
};

// Display configuration status on load
if (require.main === module) {
    console.log('📋 Configuration Status:');
    console.log('========================');
    console.log(`Database: ${getDatabaseName()}`);
    console.log(`Collection: ${config.database.collectionName}`);
    console.log(`Connection: ${config.database.connectionString.includes('mongodb') ? '✅ Configured' : '❌ Not configured'}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    
    const errors = validateConfig();
    if (errors.length > 0) {
        console.log('\n❌ Configuration Errors:');
        errors.forEach(error => console.log(`  - ${error}`));
    } else {
        console.log('\n✅ Configuration is valid');
    }
}