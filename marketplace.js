// MongoDB Atlas Fair Marketplace Demo
// This demo implements round-robin seller distribution for fair offer exposure

// Load environment variables from .env file
require('dotenv').config();

const { MongoClient } = require('mongodb');
const { faker } = require('@faker-js/faker');
const { config, validateConfig, getDatabaseName } = require('./config');

class FairMarketplace {
    constructor() {
        this.client = null;
        this.db = null;
        this.collection = null;
        this.config = config;

        // Validate configuration on initialization
        const configErrors = validateConfig();
        if (configErrors.length > 0) {
            console.error('❌ Configuration errors detected:');
            configErrors.forEach(error => console.error(`  - ${error}`));
            throw new Error('Invalid configuration. Please check your settings.');
        }
    }

    async connect() {
        try {
            this.client = new MongoClient(this.config.database.connectionString, this.config.database.options);
            await this.client.connect();
            this.db = this.client.db(getDatabaseName());
            this.collection = this.db.collection(this.config.database.collectionName);

            if (this.config.environment.enableDebugOutput) {
                console.log(`🔗 Connected to: ${getDatabaseName()}.${this.config.database.collectionName}`);
            } else {
                console.log('✅ Connected to MongoDB Atlas');
            }
        } catch (error) {
            console.error('❌ Connection failed:', error.message);
            throw error;
        }
    }

    async disconnect() {
        if (this.client) {
            await this.client.close();
            console.log('🔌 Disconnected from MongoDB Atlas');
        }
    }

    // Initialize indexes for optimal performance
    async createIndexes() {
        try {
            // Use indexes from configuration
            await this.collection.createIndexes(this.config.performance.indexes);
            console.log('✅ Indexes created successfully');
        } catch (error) {
            console.error('❌ Index creation failed:', error);
        }
    }

    // Generate sample data
    async seedData(numOffers = null, numSellers = null) {
        // Use config defaults if not specified
        numOffers = numOffers || this.config.app.seeding.defaultOfferCount;
        numSellers = numSellers || this.config.app.seeding.defaultSellerCount;

        const gameCategories = this.config.app.seeding.gameCategories;
        const sellers = Array.from({ length: numSellers }, (_, i) => `seller_${i + 1}`);

        const offers = [];

        for (let i = 0; i < numOffers; i++) {
            const gameCategory = faker.helpers.arrayElement(gameCategories);
            const sellerId = faker.helpers.arrayElement(sellers);

            offers.push({
                _id: `offer_${i + 1}`,
                title: faker.commerce.productName(),
                gameCategory,
                sellerId,
                price: parseFloat(faker.commerce.price({
                    min: this.config.app.seeding.priceRange.min,
                    max: this.config.app.seeding.priceRange.max
                })),
                description: faker.commerce.productDescription(),
                rating: parseFloat((Math.random() * 5).toFixed(1)),
                stock: faker.number.int({
                    min: this.config.app.seeding.stockRange.min,
                    max: this.config.app.seeding.stockRange.max
                }),
                createdAt: faker.date.recent({ days: this.config.app.seeding.dataRetentionDays }),
                tags: faker.helpers.arrayElements(['multiplayer', 'singleplayer', 'co-op', 'pvp', 'pve'], { min: 1, max: 3 })
            });
        }

        try {
            await this.collection.deleteMany({});
            await this.collection.insertMany(offers);
            console.log(`✅ Seeded ${numOffers} offers from ${numSellers} sellers`);
        } catch (error) {
            console.error('❌ Seeding failed:', error);
        }
    }

    // Method 1: Hash-Based Round-Robin Distribution
    async getOffersHashRoundRobin(gameCategory, page = 1, limit = null, randomSeed = null) {
        limit = limit || this.config.app.defaultPageSize;
        const skip = (page - 1) * limit;

        // Use timestamp-based seed for pagination stability
        const seedInterval = this.config.app.roundRobin.seedUpdateIntervalMinutes * 60000;
        const seed = randomSeed || Math.floor(Date.now() / seedInterval);

        try {
            const pipeline = [
                // Match game category
                { $match: { gameCategory } },

                // Add seller hash for fair distribution
                {
                    $addFields: {
                        sellerHash: {
                            $mod: [
                                {
                                    $multiply: [
                                        {
                                            $add: [
                                                {
                                                    $toInt: {
                                                        $cond: {
                                                            if: { $gt: [{ $strLenCP: "$sellerId" }, 7] },
                                                            then: { $substr: ["$sellerId", 7, -1] },
                                                            else: "1"
                                                        }
                                                    }
                                                },
                                                seed
                                            ]
                                        },
                                        { $add: [seed, 37] } // Use seed+37 as multiplier for better scrambling
                                    ]
                                },
                                10000
                            ]
                        },
                        seedUsed: seed
                    }
                },

                // Sort by seller hash (for distribution) then by quality
                { $sort: { sellerHash: 1, rating: -1, createdAt: -1 } },

                // // Remove helper field
                // { $project: { sellerHash: 0 } },

                // Apply pagination
                { $skip: skip },
                { $limit: limit }
            ];

            const options = this.config.performance.aggregation;
            const offers = await this.collection.aggregate(pipeline, options).toArray();

            const totalCount = await this.collection.countDocuments({ gameCategory });

            return {
                offers,
                pagination: {
                    page,
                    limit,
                    total: totalCount,
                    totalPages: Math.ceil(totalCount / limit),
                    hasNextPage: page * limit < totalCount,
                    hasPrevPage: page > 1
                },
                seed,
                method: 'Hash-Based Round-Robin'
            };
        } catch (error) {
            console.error('❌ Hash round-robin query failed:', error);
            throw error;
        }
    }

    // Method 2: True Round-Robin using JavaScript processing
    async getOffersTrueRoundRobin(gameCategory, page = 1, limit = null, randomSeed = null) {
        limit = limit || this.config.app.defaultPageSize;
        const skip = (page - 1) * limit;

        const seedInterval = this.config.app.roundRobin.seedUpdateIntervalMinutes * 60000;
        const seed = randomSeed || Math.floor(Date.now() / seedInterval);

        try {
            // Get all offers grouped by seller
            const pipeline = [
                { $match: { gameCategory } },
                { $sort: { rating: -1, createdAt: -1 } },
                {
                    $group: {
                        _id: "$sellerId",
                        offers: { $push: "$$ROOT" },
                        sellerOrder: {
                            $first: {
                                $mod: [
                                    { $add: [{ $toInt: { $substr: ["$sellerId", 7, -1] } }, seed] },
                                    1000
                                ]
                            }
                        }
                    }
                },
                { $sort: { sellerOrder: 1 } }
            ];

            const sellerGroups = await this.collection.aggregate(pipeline).toArray();

            // Perform true round-robin distribution
            const roundRobinOffers = [];
            const maxOffersPerSeller = Math.max(...sellerGroups.map(group => group.offers.length));

            // Round-robin: take one offer from each seller per round
            for (let round = 0; round < maxOffersPerSeller; round++) {
                for (const sellerGroup of sellerGroups) {
                    if (sellerGroup.offers[round]) {
                        roundRobinOffers.push(sellerGroup.offers[round]);
                    }
                }
            }

            // Apply pagination
            const paginatedOffers = roundRobinOffers.slice(skip, skip + limit);

            const totalCount = await this.collection.countDocuments({ gameCategory });

            return {
                offers: paginatedOffers,
                pagination: {
                    page,
                    limit,
                    total: totalCount,
                    totalPages: Math.ceil(totalCount / limit),
                    hasNextPage: page * limit < totalCount,
                    hasPrevPage: page > 1
                },
                seed,
                method: 'True Round-Robin (JavaScript)'
            };
        } catch (error) {
            console.error('❌ True round-robin query failed:', error);
            throw error;
        }
    }

    // Method 3: Weighted Random Distribution
    async getOffersWeightedRandom(gameCategory, page = 1, limit = null) {
        limit = limit || this.config.app.defaultPageSize;
        const skip = (page - 1) * limit;

        try {
            const pipeline = [
                { $match: { gameCategory } },

                // Get seller statistics
                {
                    $lookup: {
                        from: this.config.database.collectionName,
                        let: { currentSeller: "$sellerId", currentCategory: "$gameCategory" },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                            { $eq: ["$sellerId", "$$currentSeller"] },
                                            { $eq: ["$gameCategory", "$$currentCategory"] }
                                        ]
                                    }
                                }
                            },
                            { $count: "count" }
                        ],
                        as: "sellerStats"
                    }
                },

                // Add weighted score
                {
                    $addFields: {
                        sellerOfferCount: { $arrayElemAt: ["$sellerStats.count", 0] },
                        randomScore: { $rand: {} }
                    }
                },

                // Weight inversely to seller's offer count
                {
                    $addFields: {
                        weightedScore: {
                            $add: [
                                { $multiply: ["$randomScore", 100] },
                                { $divide: [100, { $add: ["$sellerOfferCount", 1] }] }
                            ]
                        }
                    }
                },

                { $sort: { weightedScore: -1 } },
                { $skip: skip },
                { $limit: limit },

                // Clean up helper fields
                { $project: { sellerStats: 0, sellerOfferCount: 0, randomScore: 0, weightedScore: 0 } }
            ];

            const options = this.config.performance.aggregation;
            const offers = await this.collection.aggregate(pipeline, options).toArray();
            const totalCount = await this.collection.countDocuments({ gameCategory });

            return {
                offers,
                pagination: {
                    page,
                    limit,
                    total: totalCount,
                    totalPages: Math.ceil(totalCount / limit),
                    hasNextPage: page * limit < totalCount,
                    hasPrevPage: page > 1
                },
                method: 'Weighted Random Distribution'
            };
        } catch (error) {
            console.error('❌ Weighted random query failed:', error);
            throw error;
        }
    }

    // Method 4: Quota-Based Distribution
    async getOffersQuotaBased(gameCategory, page = 1, limit = null, maxPerSeller = null) {
        limit = limit || this.config.app.defaultPageSize;
        maxPerSeller = maxPerSeller || this.config.app.quota.defaultMaxOffersPerSeller;

        // Validate maxPerSeller
        if (maxPerSeller > this.config.app.quota.maxMaxOffersPerSeller) {
            maxPerSeller = this.config.app.quota.maxMaxOffersPerSeller;
        }

        const skip = (page - 1) * limit;

        try {
            // Get top offers from each seller
            const pipeline = [
                { $match: { gameCategory } },
                { $sort: { rating: -1, createdAt: -1 } },

                // Group by seller and take top offers
                {
                    $group: {
                        _id: "$sellerId",
                        topOffers: { $push: "$$ROOT" },
                        randomOrder: { $first: { $rand: {} } }
                    }
                },

                // Limit offers per seller
                {
                    $project: {
                        topOffers: { $slice: ["$topOffers", maxPerSeller] },
                        randomOrder: 1
                    }
                },

                // Randomize seller order
                { $sort: { randomOrder: 1 } },

                // Flatten offers
                { $unwind: "$topOffers" },
                { $replaceRoot: { newRoot: "$topOffers" } },

                { $skip: skip },
                { $limit: limit }
            ];

            const options = this.config.performance.aggregation;
            const offers = await this.collection.aggregate(pipeline, options).toArray();
            const totalCount = await this.collection.countDocuments({ gameCategory });

            return {
                offers,
                pagination: {
                    page,
                    limit,
                    total: totalCount,
                    totalPages: Math.ceil(totalCount / limit),
                    hasNextPage: page * limit < totalCount,
                    hasPrevPage: page > 1
                },
                maxPerSeller,
                method: 'Quota-Based Distribution'
            };
        } catch (error) {
            console.error('❌ Quota-based query failed:', error);
            throw error;
        }
    }

    // Get seller distribution statistics
    async getSellerStats(gameCategory) {
        try {
            const pipeline = [
                { $match: gameCategory ? { gameCategory } : {} },
                {
                    $group: {
                        _id: "$sellerId",
                        offerCount: { $sum: 1 },
                        avgPrice: { $avg: "$price" },
                        avgRating: { $avg: "$rating" },
                        totalStock: { $sum: "$stock" }
                    }
                },
                { $sort: { offerCount: -1 } }
            ];

            const stats = await this.collection.aggregate(pipeline).toArray();

            console.log(`\n📊 Seller Statistics ${gameCategory ? `for ${gameCategory}` : '(All Categories)'}`);
            console.log('='.repeat(60));
            stats.forEach(seller => {
                console.log(`Seller: ${seller._id.padEnd(12)} | Offers: ${seller.offerCount.toString().padStart(3)} | Avg Price: $${seller.avgPrice.toFixed(2)} | Avg Rating: ${seller.avgRating.toFixed(1)}`);
            });

            return stats;
        } catch (error) {
            console.error('❌ Stats query failed:', error);
            throw error;
        }
    }

    // Compare different distribution methods
    async compareDistributionMethods(gameCategory, limit = 20) {
        console.log(`\n🔬 Comparing Distribution Methods for ${gameCategory}`);
        console.log('='.repeat(70));

        const methods = [
            { name: 'Hash Round-Robin', fn: () => this.getOffersHashRoundRobin(gameCategory, 1, limit) },
            { name: 'True Round-Robin', fn: () => this.getOffersTrueRoundRobin(gameCategory, 1, limit) },
            { name: 'Weighted Random', fn: () => this.getOffersWeightedRandom(gameCategory, 1, limit) },
            { name: 'Quota-Based', fn: () => this.getOffersQuotaBased(gameCategory, 1, limit, 3) }
        ];

        for (const method of methods) {
            try {
                const result = await method.fn();
                const sellerCounts = {};

                result.offers.forEach(offer => {
                    sellerCounts[offer.sellerId] = (sellerCounts[offer.sellerId] || 0) + 1;
                });

                console.log(`\n${method.name}:`);
                console.log(`  Unique sellers: ${Object.keys(sellerCounts).length}`);
                console.log(`  Distribution: ${JSON.stringify(sellerCounts, null, 2)}`);

                // Calculate distribution fairness (lower = more fair)
                const counts = Object.values(sellerCounts);
                const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
                const variance = counts.reduce((sum, count) => sum + Math.pow(count - avg, 2), 0) / counts.length;
                console.log(`  Fairness Score: ${variance.toFixed(2)} (lower = more fair)`);

            } catch (error) {
                console.error(`  ❌ ${method.name} failed:`, error.message);
            }
        }
    }

    // Display results in a formatted table
    displayResults(result) {
        const tableWidth = this.config.display.resultsTableWidth;
        const titleLength = this.config.display.maxTitleLength;
        const sellerLength = this.config.display.maxSellerNameLength;
        const priceWidth = this.config.display.priceDisplayWidth;

        console.log(`\n📋 Results - ${result.method}`);
        console.log('='.repeat(tableWidth));
        console.log(`Page ${result.pagination.page} of ${result.pagination.totalPages} | Total: ${result.pagination.total} offers`);
        if (result.seed) console.log(`Seed: ${result.seed}`);
        console.log('-'.repeat(tableWidth));

        result.offers.forEach((offer, index) => {
            console.log(`${(index + 1).toString().padStart(2)}. ${offer.title.padEnd(titleLength)} | ${offer.sellerId.padEnd(sellerLength)} | $${offer.price.toString().padStart(priceWidth)} | ⭐${offer.rating}`);
        });

        // Show seller distribution in current page
        const sellerCounts = {};
        result.offers.forEach(offer => {
            sellerCounts[offer.sellerId] = (sellerCounts[offer.sellerId] || 0) + 1;
        });

        console.log('-'.repeat(tableWidth));
        console.log('Seller distribution on this page:');
        Object.entries(sellerCounts)
            .sort(([, a], [, b]) => b - a)
            .forEach(([seller, count]) => {
                console.log(`  ${seller}: ${count} offer${count > 1 ? 's' : ''}`);
            });
    }
}

// CLI Command Handler
class CLIHandler {
    constructor() {
        this.marketplace = new FairMarketplace();
    }

    async init() {
        await this.marketplace.connect();
        await this.marketplace.createIndexes();
    }

    async cleanup() {
        await this.marketplace.disconnect();
    }

    async handleCommand(args) {
        const command = args[2];

        try {
            switch (command) {
                case 'config':
                    this.showConfigInfo();
                    break;

                case 'seed':
                    const numOffers = parseInt(args[3]) || this.marketplace.config.app.seeding.defaultOfferCount;
                    const numSellers = parseInt(args[4]) || this.marketplace.config.app.seeding.defaultSellerCount;
                    await this.marketplace.seedData(numOffers, numSellers);
                    break;

                case 'search':
                    const gameCategory = args[3] || this.marketplace.config.app.seeding.gameCategories[0];
                    const method = args[4] || 'hash-round-robin';
                    const page = parseInt(args[5]) || 1;
                    const limit = parseInt(args[6]) || this.marketplace.config.app.defaultPageSize;
                    const customSeed = args[7] ? parseInt(args[7]) : null;

                    let result;
                    switch (method) {
                        case 'hash-round-robin':
                        case 'round-robin':
                            result = await this.marketplace.getOffersHashRoundRobin(gameCategory, page, limit, customSeed);
                            break;
                        case 'true-round-robin':
                            result = await this.marketplace.getOffersTrueRoundRobin(gameCategory, page, limit, customSeed);
                            break;
                        case 'weighted':
                            result = await this.marketplace.getOffersWeightedRandom(gameCategory, page, limit);
                            break;
                        case 'quota':
                            const maxPerSeller = parseInt(args[7]) || this.marketplace.config.app.quota.defaultMaxOffersPerSeller;
                            result = await this.marketplace.getOffersQuotaBased(gameCategory, page, limit, maxPerSeller);
                            break;
                        default:
                            console.log('❌ Unknown method. Use: hash-round-robin, true-round-robin, weighted, or quota');
                            return;
                    }

                    this.marketplace.displayResults(result);
                    break;

                case 'stats':
                    const statsCategory = args[3];
                    await this.marketplace.getSellerStats(statsCategory);
                    break;

                case 'compare':
                    const compareCategory = args[3] || this.marketplace.config.app.seeding.gameCategories[0];
                    const compareLimit = parseInt(args[4]) || 20;
                    await this.marketplace.compareDistributionMethods(compareCategory, compareLimit);
                    break;

                case 'debug':
                    const debugCategory = args[3] || this.marketplace.config.app.seeding.gameCategories[0];
                    await this.debugRoundRobin(debugCategory);
                    break;

                case 'help':
                    this.showHelp();
                    break;

                default:
                    console.log('❌ Unknown command. Use "help" for available commands.');
            }
        } catch (error) {
            console.error('❌ Command failed:', error.message);
        }
    }

    async debugRoundRobin(gameCategory) {
        console.log(`\n🔍 Debugging Round-Robin for ${gameCategory}`);
        console.log('='.repeat(50));

        try {
            // Check basic data availability
            const totalOffers = await this.marketplace.collection.countDocuments({ gameCategory });
            console.log(`Total offers in ${gameCategory}: ${totalOffers}`);

            if (totalOffers === 0) {
                console.log('❌ No offers found. Run "node marketplace.js seed" first.');
                return;
            }

            // Check seller distribution
            const sellerStats = await this.marketplace.collection.aggregate([
                { $match: { gameCategory } },
                {
                    $group: {
                        _id: "$sellerId",
                        count: { $sum: 1 }
                    }
                },
                { $sort: { count: -1 } }
            ]).toArray();

            console.log(`\nSellers in ${gameCategory}:`);
            sellerStats.forEach(seller => {
                console.log(`  ${seller._id}: ${seller.count} offers`);
            });

            // Test both round-robin methods
            console.log('\n🧪 Testing hash round-robin...');
            const hashResult = await this.marketplace.getOffersHashRoundRobin(gameCategory, 1, 5);
            console.log(`Hash round-robin returned ${hashResult.offers.length} offers`);

            console.log('\n🧪 Testing true round-robin...');
            const trueResult = await this.marketplace.getOffersTrueRoundRobin(gameCategory, 1, 5);
            console.log(`True round-robin returned ${trueResult.offers.length} offers`);

            // Show distributions
            const hashDistribution = {};
            hashResult.offers.forEach(offer => {
                hashDistribution[offer.sellerId] = (hashDistribution[offer.sellerId] || 0) + 1;
            });
            console.log('Hash distribution:', hashDistribution);

            const trueDistribution = {};
            trueResult.offers.forEach(offer => {
                trueDistribution[offer.sellerId] = (trueDistribution[offer.sellerId] || 0) + 1;
            });
            console.log('True distribution:', trueDistribution);

        } catch (error) {
            console.error('❌ Debug failed:', error);
        }
    }

    showConfigInfo() {
        console.log('\n⚙️ Current Configuration:');
        console.log('='.repeat(50));
        console.log(`Database: ${getDatabaseName()}`);
        console.log(`Collection: ${this.marketplace.config.database.collectionName}`);
        console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
        console.log(`Default Page Size: ${this.marketplace.config.app.defaultPageSize}`);
        console.log(`Game Categories: ${this.marketplace.config.app.seeding.gameCategories.join(', ')}`);
        console.log(`Max Offers per Seller: ${this.marketplace.config.app.quota.defaultMaxOffersPerSeller}`);
        console.log(`Seed Update Interval: ${this.marketplace.config.app.roundRobin.seedUpdateIntervalMinutes} minutes`);
    }

    showHelp() {
        const defaultCategory = this.marketplace.config.app.seeding.gameCategories[0];
        const defaultPageSize = this.marketplace.config.app.defaultPageSize;
        const defaultMaxPerSeller = this.marketplace.config.app.quota.defaultMaxOffersPerSeller;

        console.log(`
🎮 MongoDB Atlas Fair Marketplace Demo Commands

📝 Setup:
  node marketplace.js seed [numOffers] [numSellers]
    Example: node marketplace.js seed 150 20
    Default: ${this.marketplace.config.app.seeding.defaultOfferCount} offers, ${this.marketplace.config.app.seeding.defaultSellerCount} sellers

🔍 Search:
  node marketplace.js search [category] [method] [page] [limit] [maxPerSeller]
    Methods: hash-round-robin, true-round-robin, weighted, quota
    Categories: ${this.marketplace.config.app.seeding.gameCategories.join(', ')}
    Example: node marketplace.js search ${defaultCategory} hash-round-robin 1 ${defaultPageSize}

📊 Statistics:
  node marketplace.js stats [category]
    Example: node marketplace.js stats ${defaultCategory}

🔬 Compare Methods:
  node marketplace.js compare [category] [limit]
    Example: node marketplace.js compare Strategy 25

🔍 Debug:
  node marketplace.js debug [category]
    Example: node marketplace.js debug RPG

⚙️ Configuration:
  node marketplace.js config
    Shows current configuration settings

❓ Help:
  node marketplace.js help

🔧 Environment Setup:
  Set MONGODB_URI environment variable with your Atlas connection string
  Example: set MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
  
  Optional environment variables:
  - DB_NAME (default: ${this.marketplace.config.database.databaseName})
  - COLLECTION_NAME (default: ${this.marketplace.config.database.collectionName})
  - NODE_ENV (default: development)
        `);
    }
}

// Main execution
async function main() {
    const cli = new CLIHandler();

    try {
        await cli.init();
        await cli.handleCommand(process.argv);
    } catch (error) {
        console.error('💥 Application error:', error.message);
    } finally {
        await cli.cleanup();
        process.exit(0);
    }
}

// Run if this file is executed directly
if (require.main === module) {
    main();
}

module.exports = { FairMarketplace, CLIHandler };