// MongoDB Atlas Price Conversion System
// Demonstrates real-time currency conversion with USD as peg currency

// Load environment variables from .env file
require('dotenv').config();

const { MongoClient, Decimal128 } = require('mongodb');
const { faker } = require('@faker-js/faker');
const { config, validateConfig, getDatabaseName } = require('./config');

class PriceConversionSystem {
    constructor() {
        this.client = null;
        this.db = null;
        this.listingsCollection = null;
        this.exchangeRatesCollection = null;
        this.config = config;

        // Supported currencies with USD as peg
        this.currencies = ['PHP', 'IDR', 'MYR', 'USD'];
        this.pegCurrency = 'USD';

        // Collection names
        this.listingsCollectionName = 'marketplace_listings';
        this.exchangeRatesCollectionName = 'exchange_rates';

        // In-memory cache for exchange rates
        this.exchangeRatesCache = new Map();

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
            this.listingsCollection = this.db.collection(this.listingsCollectionName);
            this.exchangeRatesCollection = this.db.collection(this.exchangeRatesCollectionName);

            if (this.config.environment.enableDebugOutput) {
                console.log(`🔗 Connected to: ${getDatabaseName()}`);
                console.log(`📋 Collections: ${this.listingsCollectionName}, ${this.exchangeRatesCollectionName}`);
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

    // Create indexes for optimal performance
    async createIndexes() {
        try {
            // Indexes for listings collection
            const listingIndexes = [
                { key: { priceUSD: 1 }, name: 'price_usd_idx' },
                { key: { currency: 1 }, name: 'currency_idx' },
                { key: { category: 1 }, name: 'category_idx' },
                { key: { priceUSD: 1, category: 1 }, name: 'price_category_idx' },
                { key: { createdAt: -1 }, name: 'created_date_idx' },
                { key: { priceUSD: 1, createdAt: -1 }, name: 'price_date_compound_idx' }
            ];

            // Indexes for exchange rates collection (updated for new schema)
            const exchangeIndexes = [
                { key: { currency: 1 }, name: 'currency_idx' },
                { key: { lastUpdated: -1 }, name: 'last_updated_idx' },
                { key: { currency: 1, lastUpdated: -1 }, name: 'currency_time_idx' }
            ];

            await this.listingsCollection.createIndexes(listingIndexes);
            await this.exchangeRatesCollection.createIndexes(exchangeIndexes);

            console.log('✅ Price conversion indexes created successfully');
        } catch (error) {
            console.error('❌ Index creation failed:', error);
        }
    }

    // Initialize or update exchange rates (only TO USD conversions)
    async initializeExchangeRates() {
        try {
            const currentTime = new Date();

            // Simplified: Only store rates TO USD (USD is always the target)
            const exchangeRates = [
                // All currencies to USD
                { currency: 'PHP', toUSDRate: Decimal128.fromString('0.0178'), lastUpdated: currentTime },
                { currency: 'IDR', toUSDRate: Decimal128.fromString('0.0000635'), lastUpdated: currentTime },
                { currency: 'MYR', toUSDRate: Decimal128.fromString('0.2247'), lastUpdated: currentTime },
                { currency: 'USD', toUSDRate: Decimal128.fromString('1.0000'), lastUpdated: currentTime }
            ];

            // Clear existing rates and insert new ones
            await this.exchangeRatesCollection.deleteMany({});
            await this.exchangeRatesCollection.insertMany(exchangeRates);

            // Update cache
            await this.refreshExchangeRateCache();

            console.log(`✅ Initialized ${exchangeRates.length} exchange rates (all to USD)`);
            return exchangeRates;
        } catch (error) {
            console.error('❌ Exchange rate initialization failed:', error);
            throw error;
        }
    }

    // Refresh exchange rate cache from database (simplified structure)
    async refreshExchangeRateCache() {
        try {
            const rates = await this.exchangeRatesCollection.find({}).toArray();
            this.exchangeRatesCache.clear();

            rates.forEach(rate => {
                // Store as currency -> USD rate
                this.exchangeRatesCache.set(rate.currency, parseFloat(rate.toUSDRate.toString()));
            });

            console.log(`🔄 Refreshed exchange rate cache with ${rates.length} rates (all to USD)`);
        } catch (error) {
            console.error('❌ Cache refresh failed:', error);
        }
    }

    // Convert price to USD using simplified cache
    convertToUSD(price, fromCurrency) {
        if (fromCurrency === 'USD') {
            return price;
        }

        const rate = this.exchangeRatesCache.get(fromCurrency);

        if (!rate) {
            console.error(`❌ Exchange rate not found for ${fromCurrency} to USD`);
            console.error(`Available rates: ${Array.from(this.exchangeRatesCache.keys()).join(', ')}`);
            console.error(`💡 Make sure to run 'node price-conversion.js setup' first to initialize exchange rates`);
            throw new Error(`Exchange rate not found for ${fromCurrency} to USD`);
        }

        return parseFloat((price * rate).toFixed(4));
    }

    // Convert USD price to target currency
    convertFromUSD(usdPrice, toCurrency) {
        if (toCurrency === 'USD') {
            return usdPrice;
        }

        const toUSDRate = this.exchangeRatesCache.get(toCurrency);

        if (!toUSDRate) {
            throw new Error(`Exchange rate not found for ${toCurrency}`);
        }

        // To convert FROM USD, we divide by the TO USD rate
        const fromUSDRate = 1 / toUSDRate;
        return parseFloat((usdPrice * fromUSDRate).toFixed(4));
    }

    // Generate marketplace listings with different currencies
    async generateListings(count = 50) {
        try {
            // Check if exchange rates are available
            if (this.exchangeRatesCache.size === 0) {
                console.log('⚠️ No exchange rates found. Initializing exchange rates first...');
                await this.initializeExchangeRates();
            }

            const categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Automotive'];
            const listings = [];

            for (let i = 1; i <= count; i++) {
                // Random currency selection
                const currency = faker.helpers.arrayElement(this.currencies);

                // Generate price based on currency (realistic price ranges)
                let localPrice;
                switch (currency) {
                    case 'PHP':
                        localPrice = parseFloat(faker.commerce.price({ min: 500, max: 50000, dec: 2 }));
                        break;
                    case 'IDR':
                        localPrice = parseFloat(faker.commerce.price({ min: 100000, max: 5000000, dec: 0 }));
                        break;
                    case 'MYR':
                        localPrice = parseFloat(faker.commerce.price({ min: 10, max: 2000, dec: 2 }));
                        break;
                    case 'USD':
                        localPrice = parseFloat(faker.commerce.price({ min: 5, max: 1000, dec: 2 }));
                        break;
                    default:
                        localPrice = parseFloat(faker.commerce.price({ min: 10, max: 500, dec: 2 }));
                }

                // Convert to USD (peg currency)
                const priceUSD = this.convertToUSD(localPrice, currency);

                const listing = {
                    title: faker.commerce.productName(),
                    description: faker.commerce.productDescription(),
                    category: faker.helpers.arrayElement(categories),

                    // Price information
                    priceLocal: Decimal128.fromString(localPrice.toFixed(currency === 'IDR' ? 0 : 2)),
                    currency: currency,
                    priceUSD: Decimal128.fromString(priceUSD.toFixed(4)), // Peg currency for sorting

                    // Additional fields
                    sellerId: `seller_${faker.number.int({ min: 1, max: 20 })}`,
                    location: this.getLocationByCurrency(currency),
                    condition: faker.helpers.arrayElement(['New', 'Like New', 'Good', 'Fair']),
                    inStock: faker.datatype.boolean(),

                    // Timestamps
                    createdAt: faker.date.recent({ days: 30 }),
                    updatedAt: new Date(),

                    // Conversion metadata
                    conversionInfo: {
                        originalCurrency: currency,
                        originalPrice: localPrice,
                        usdConversionRate: this.exchangeRatesCache.get(currency),
                        convertedAt: new Date()
                    }
                };

                listings.push(listing);
            }

            // Insert new listings (don't clear existing ones)
            const result = await this.listingsCollection.insertMany(listings);

            console.log(`✅ Generated ${result.insertedCount} marketplace listings`);
            console.log(`💱 Currencies: ${this.currencies.join(', ')} (USD is peg currency)`);

            return result;
        } catch (error) {
            console.error('❌ Listing generation failed:', error);
            throw error;
        }
    }

    // Get typical location based on currency
    getLocationByCurrency(currency) {
        const locations = {
            'PHP': ['Manila', 'Cebu', 'Davao', 'Quezon City'],
            'IDR': ['Jakarta', 'Surabaya', 'Bandung', 'Medan'],
            'MYR': ['Kuala Lumpur', 'Penang', 'Johor Bahru', 'Ipoh'],
            'USD': ['New York', 'Los Angeles', 'Chicago', 'Houston']
        };

        return faker.helpers.arrayElement(locations[currency] || ['Unknown']);
    }

    // List all listings sorted by price with performance monitoring and pagination
    async listByPrice(inputCurrency = 'USD', sortOrder = 1, limit = 20, category = null, page = 1) {
        try {
            const skip = (page - 1) * limit;
            const query = category ? { category } : {};

            console.log(`\n🔍 Query Performance Analysis:`);
            console.log(`📄 Page: ${page}, Limit: ${limit}, Skip: ${skip}`);
            console.log(`💱 Input Currency: ${inputCurrency}, Sort: ${sortOrder === 1 ? 'ASC' : 'DESC'}`);
            console.log(`📂 Category Filter: ${category || 'None'}`);

            const startTime = Date.now();

            // ALWAYS use USD for database operations (fast, indexed)
            const sort = { priceUSD: sortOrder };

            // Get explain plan
            const explainResult = await this.listingsCollection
                .find(query)
                .sort(sort)
                .skip(skip)
                .limit(limit)
                .explain('executionStats');

            // Execute the actual query
            const listings = await this.listingsCollection
                .find(query)
                .sort(sort)
                .skip(skip)
                .limit(limit)
                .toArray();

            const endTime = Date.now();
            const queryTime = endTime - startTime;

            // Get total count for pagination
            const totalCount = await this.listingsCollection.countDocuments(query);
            const totalPages = Math.ceil(totalCount / limit);

            // Display performance metrics
            this.displayPerformanceMetrics(explainResult, queryTime, totalCount);

            // Display results with pagination info
            this.displayListings(listings, inputCurrency, sortOrder, category, {
                page,
                limit,
                skip,
                totalCount,
                totalPages,
                hasNextPage: page < totalPages,
                hasPrevPage: page > 1
            });

            return {
                listings,
                pagination: {
                    page,
                    limit,
                    skip,
                    totalCount,
                    totalPages,
                    hasNextPage: page < totalPages,
                    hasPrevPage: page > 1
                },
                performance: {
                    queryTime,
                    indexUsed: this.extractIndexName(explainResult),
                    docsExamined: explainResult.executionStats.totalDocsExamined,
                    docsReturned: listings.length
                }
            };

        } catch (error) {
            console.error('❌ Listing retrieval failed:', error);
            throw error;
        }
    }

    // Search listings within price range with performance monitoring and pagination
    async searchByPriceRange(minPrice, maxPrice, currency = 'USD', category = null, limit = 50, page = 1) {
        try {
            const skip = (page - 1) * limit;
            let query;

            console.log(`\n🔍 Search Performance Analysis:`);
            console.log(`📄 Page: ${page}, Limit: ${limit}, Skip: ${skip}`);
            console.log(`💱 Currency: ${currency}, Range: ${minPrice} - ${maxPrice}`);
            console.log(`📂 Category Filter: ${category || 'None'}`);

            const startTime = Date.now();

            if (currency === 'USD') {
                // Direct USD range query (most efficient)
                console.log(`🔄 Using direct USD range query (most efficient)`);
                query = {
                    priceUSD: {
                        $gte: Decimal128.fromString(minPrice.toString()),
                        $lte: Decimal128.fromString(maxPrice.toString())
                    }
                };
            } else {
                // Convert input currency range to USD for efficient querying
                const minUSD = this.convertToUSD(minPrice, currency);
                const maxUSD = this.convertToUSD(maxPrice, currency);

                console.log(`🔄 Converted ${currency} range ${this.formatPrice(minPrice.toString(), currency)} - ${this.formatPrice(maxPrice.toString(), currency)} to USD range $${minUSD.toFixed(4)} - $${maxUSD.toFixed(4)}`);

                query = {
                    priceUSD: {
                        $gte: Decimal128.fromString(minUSD.toString()),
                        $lte: Decimal128.fromString(maxUSD.toString())
                    }
                };
            }

            if (category != 'All') {
                query.category = category;
            }

            // Get explain plan
            const explainResult = await this.listingsCollection
                .find(query)
                .sort({ priceUSD: 1 })
                .skip(skip)
                .limit(limit)
                .explain('executionStats');

            // Execute the actual query
            console.log(`⚡ Using optimized USD index for range query`);
            const listings = await this.listingsCollection
                .find(query)
                .sort({ priceUSD: 1 })
                .skip(skip)
                .limit(limit)
                .toArray();

            const endTime = Date.now();
            const queryTime = endTime - startTime;

            // Get total count for pagination
            const totalCount = await this.listingsCollection.countDocuments(query);
            const totalPages = Math.ceil(totalCount / limit);

            // Display performance metrics
            this.displayPerformanceMetrics(explainResult, queryTime, totalCount);

            console.log(`\n🔍 Listings in ${currency} Range: ${this.formatPrice(minPrice.toString(), currency)} - ${this.formatPrice(maxPrice.toString(), currency)}`);
            if (category) console.log(`📂 Category: ${category}`);
            console.log(`📊 Found ${totalCount} listings total, showing page ${page} of ${totalPages}`);
            console.log('='.repeat(160));
            console.log(`${'Title'.padEnd(25)} | ${'Local Price'.padEnd(18)} | ${(currency + ' Price').padEnd(15)} | ${'USD Price'.padEnd(12)} | ${'Category'.padEnd(12)} | ${'Location'.padEnd(12)}`);
            console.log('-'.repeat(160));

            listings.forEach(listing => {
                const title = (listing.title.length > 25 ? listing.title.substring(0, 22) + '...' : listing.title).padEnd(25);
                const localPrice = this.formatPrice(listing.priceLocal.toString(), listing.currency).padEnd(18);

                // Convert USD price to display currency
                let displayPrice;
                if (currency === 'USD') {
                    displayPrice = this.formatPrice(listing.priceUSD.toString(), 'USD');
                } else {
                    const usdPrice = parseFloat(listing.priceUSD.toString());
                    const convertedPrice = this.convertFromUSD(usdPrice, currency);
                    displayPrice = this.formatPrice(convertedPrice.toString(), currency);
                }

                const usdPrice = ('$' + parseFloat(listing.priceUSD.toString()).toFixed(2)).padEnd(12);
                const categoryCol = listing.category.padEnd(12);
                const location = listing.location.padEnd(12);

                console.log(`${title} | ${localPrice} | ${displayPrice.padEnd(15)} | ${usdPrice} | ${categoryCol} | ${location}`);
            });

            console.log('-'.repeat(160));
            console.log(`📄 Page ${page} of ${totalPages} | Total: ${totalCount} listings | Query time: ${queryTime}ms`);
            if (page < totalPages) console.log(`➡️  Next page: add parameter ${page + 1} at the end`);
            if (page > 1) console.log(`⬅️  Previous page: change parameter to ${page - 1}`);

            return {
                listings,
                pagination: {
                    page,
                    limit,
                    skip,
                    totalCount,
                    totalPages,
                    hasNextPage: page < totalPages,
                    hasPrevPage: page > 1
                },
                performance: {
                    queryTime,
                    indexUsed: this.extractIndexName(explainResult),
                    docsExamined: explainResult.executionStats.totalDocsExamined,
                    docsReturned: listings.length
                }
            };
        } catch (error) {
            console.error('❌ Price range search failed:', error);
            throw error;
        }
    }

    // Display listings helper method with pagination
    displayListings(listings, inputCurrency, sortOrder, category, pagination) {
        console.log(`\n💰 Listings Sorted by ${inputCurrency} Price (${sortOrder === 1 ? 'Low to High' : 'High to Low'})`);
        console.log(`⚡ Performance: Used USD index for sorting, converted to ${inputCurrency} for display only`);
        if (category) console.log(`📂 Category: ${category}`);

        if (pagination) {
            console.log(`📄 Page ${pagination.page} of ${pagination.totalPages} | Total: ${pagination.totalCount} listings`);
        }

        console.log('='.repeat(160));
        console.log(`${'Title'.padEnd(25)} | ${'Local Price'.padEnd(15)} | ${'Currency'.padEnd(8)} | ${(inputCurrency + ' Price').padEnd(15)} | ${'USD Price'.padEnd(12)} | ${'Category'.padEnd(12)} | ${'Location'.padEnd(12)}`);
        console.log('-'.repeat(160));

        listings.forEach((listing, index) => {
            const title = (listing.title.length > 25 ? listing.title.substring(0, 22) + '...' : listing.title).padEnd(25);
            const localPrice = this.formatPrice(listing.priceLocal.toString(), listing.currency).padEnd(15);
            const currency = listing.currency.padEnd(8);

            // Convert USD price to display currency (fast conversion, not for sorting)
            let displayPrice;
            if (inputCurrency === 'USD') {
                displayPrice = this.formatPrice(listing.priceUSD.toString(), 'USD');
            } else {
                // Convert USD to display currency for UI only
                const usdPrice = parseFloat(listing.priceUSD.toString());
                const convertedPrice = this.convertFromUSD(usdPrice, inputCurrency);
                displayPrice = this.formatPrice(convertedPrice.toString(), inputCurrency);
            }

            // Show USD price for transparency
            const usdPrice = ('$' + parseFloat(listing.priceUSD.toString()).toFixed(2)).padEnd(12);
            const categoryCol = listing.category.padEnd(12);
            const location = listing.location.padEnd(12);

            console.log(`${title} | ${localPrice} | ${currency} | ${displayPrice.padEnd(15)} | ${usdPrice} | ${categoryCol} | ${location}`);
        });

        console.log('-'.repeat(160));

        if (pagination) {
            console.log(`📄 Page ${pagination.page} of ${pagination.totalPages} | Showing ${listings.length} of ${pagination.totalCount} total listings`);
            if (pagination.hasNextPage) console.log(`➡️  Next page: add parameter ${pagination.page + 1} at the end`);
            if (pagination.hasPrevPage) console.log(`⬅️  Previous page: change parameter to ${pagination.page - 1}`);
        }

        console.log(`💡 Note: Sorted by USD price in database (fast), displayed in ${inputCurrency} (conversion only for UI)`);
    }

    // Display performance metrics from explain plan
    displayPerformanceMetrics(explainResult, queryTime, totalDocs) {
        console.log(`\n📊 Query Performance Metrics:`);
        console.log('='.repeat(60));

        const executionStats = explainResult.executionStats;
        const indexUsed = this.extractIndexName(explainResult);

        console.log(`⚡ Query Execution Time: ${queryTime}ms`);
        console.log(`🔍 Index Used: ${indexUsed}`);
        console.log(`📄 Documents Examined: ${executionStats.totalDocsExamined}`);
        console.log(`📋 Documents Returned: ${executionStats.executionStages.advanced || 0}`);
        console.log(`🎯 Total Documents in Collection: ${totalDocs}`);
        console.log(`📈 Efficiency Ratio: ${((executionStats.executionStages.advanced || 0) / Math.max(executionStats.totalDocsExamined, 1) * 100).toFixed(2)}%`);

        // Performance analysis
        if (executionStats.totalDocsExamined > (executionStats.executionStages.advanced || 0) * 10) {
            console.log(`⚠️  Performance Warning: High document examination ratio. Consider adding indexes.`);
        } else {
            console.log(`✅ Good Performance: Efficient index usage detected.`);
        }

        console.log('='.repeat(60));
    }

    // Extract index name from explain plan
    extractIndexName(explainResult) {
        const winningPlan = explainResult.queryPlanner.winningPlan;
        let stage = winningPlan;

        // Navigate through the plan to find index usage
        while (stage) {
            if (stage.stage === 'IXSCAN') {
                return stage.indexName || 'Index Scan';
            } else if (stage.inputStage) {
                stage = stage.inputStage;
            } else if (stage.inputStages && stage.inputStages.length > 0) {
                stage = stage.inputStages[0];
            } else {
                break;
            }
        }

        return 'Collection Scan (No Index)';
    }

    // Analyze price distribution across currencies
    async analyzePriceDistribution() {
        try {
            console.log('\n📊 Price Distribution Analysis');
            console.log('='.repeat(80));

            // Overall statistics
            const overallStats = await this.listingsCollection.aggregate([
                {
                    $group: {
                        _id: null,
                        totalListings: { $sum: 1 },
                        avgUSDPrice: { $avg: '$priceUSD' },
                        minUSDPrice: { $min: '$priceUSD' },
                        maxUSDPrice: { $max: '$priceUSD' },
                        totalUSDValue: { $sum: '$priceUSD' }
                    }
                }
            ]).toArray();

            if (overallStats.length > 0) {
                const stats = overallStats[0];
                console.log(`Total Listings: ${stats.totalListings}`);
                console.log(`Average USD Price: $${stats.avgUSDPrice.toString()}`);
                console.log(`Price Range: $${stats.minUSDPrice.toString()} - $${stats.maxUSDPrice.toString()}`);
                console.log(`Total Market Value: $${stats.totalUSDValue.toString()}`);
            }

            // By currency breakdown
            const currencyStats = await this.listingsCollection.aggregate([
                {
                    $group: {
                        _id: '$currency',
                        count: { $sum: 1 },
                        avgLocalPrice: { $avg: '$priceLocal' },
                        avgUSDPrice: { $avg: '$priceUSD' },
                        minUSDPrice: { $min: '$priceUSD' },
                        maxUSDPrice: { $max: '$priceUSD' }
                    }
                },
                { $sort: { count: -1 } }
            ]).toArray();

            console.log('\n💱 By Currency:');
            currencyStats.forEach(curr => {
                console.log(`\n  ${curr._id}:`);
                console.log(`    Listings: ${curr.count}`);
                console.log(`    Avg Local Price: ${this.formatPrice(curr.avgLocalPrice.toString(), curr._id)}`);
                console.log(`    Avg USD Price: $${curr.avgUSDPrice.toString()}`);
                console.log(`    USD Range: $${curr.minUSDPrice.toString()} - $${curr.maxUSDPrice.toString()}`);
            });

            // By category breakdown
            const categoryStats = await this.listingsCollection.aggregate([
                {
                    $group: {
                        _id: '$category',
                        count: { $sum: 1 },
                        avgUSDPrice: { $avg: '$priceUSD' }
                    }
                },
                { $sort: { avgUSDPrice: -1 } }
            ]).toArray();

            console.log('\n📂 By Category (sorted by avg USD price):');
            categoryStats.forEach(cat => {
                console.log(`  ${cat._id}: ${cat.count} listings, avg $${cat.avgUSDPrice.toString()}`);
            });

            return { overallStats: overallStats[0], currencyStats, categoryStats };
        } catch (error) {
            console.error('❌ Analysis failed:', error);
            throw error;
        }
    }

    // Update exchange rates (simulates real-time rate updates)
    async updateExchangeRates() {
        try {
            const currentTime = new Date();
            const fluctuation = 0.02; // 2% max fluctuation

            console.log('🔄 Updating exchange rates...');

            // Get current rates
            const currentRates = await this.exchangeRatesCollection.find({}).toArray();

            for (const rate of currentRates) {
                if (rate.currency === 'USD') {
                    continue; // Skip USD (always 1.0000)
                }

                // Apply random fluctuation
                const currentRate = parseFloat(rate.toUSDRate.toString());
                const change = (Math.random() - 0.5) * 2 * fluctuation; // -2% to +2%
                const newRate = currentRate * (1 + change);

                // Update the rate
                await this.exchangeRatesCollection.updateOne(
                    { _id: rate._id },
                    {
                        $set: {
                            toUSDRate: Decimal128.fromString(newRate.toFixed(rate.currency === 'IDR' ? 8 : 6)),
                            lastUpdated: currentTime,
                            previousRate: rate.toUSDRate,
                            changePercent: Decimal128.fromString((change * 100).toFixed(2))
                        }
                    }
                );

                console.log(`  ${rate.currency}/USD: ${currentRate.toFixed(6)} → ${newRate.toFixed(6)} (${(change * 100).toFixed(2)}%)`);
            }

            // Refresh cache
            await this.refreshExchangeRateCache();

            console.log('✅ Exchange rates updated');
        } catch (error) {
            console.error('❌ Exchange rate update failed:', error);
        }
    }

    // Recalculate all USD prices after rate changes
    async recalculateUSDPrices() {
        try {
            console.log('🔄 Recalculating USD prices for all listings...');

            const listings = await this.listingsCollection.find({}).toArray();

            for (const listing of listings) {
                if (listing.currency === 'USD') {
                    continue; // Skip USD listings
                }

                const localPrice = parseFloat(listing.priceLocal.toString());
                const newUSDPrice = this.convertToUSD(localPrice, listing.currency);

                await this.listingsCollection.updateOne(
                    { _id: listing._id },
                    {
                        $set: {
                            priceUSD: Decimal128.fromString(newUSDPrice.toFixed(4)),
                            'conversionInfo.usdConversionRate': this.exchangeRatesCache.get(listing.currency),
                            'conversionInfo.convertedAt': new Date()
                        }
                    }
                );
            }

            console.log(`✅ Recalculated USD prices for ${listings.length} listings`);
        } catch (error) {
            console.error('❌ USD price recalculation failed:', error);
        }
    }

    // Alternative version using pure MongoDB aggregation pipeline (even more efficient for very large datasets)
    async recalculateUSDPricesAggregation(updateSinceDate = null, limit = null) {
        try {
            const startTime = Date.now();
            console.log('🔄 Starting USD price recalculation using pure MongoDB aggregation...');

            // Build match criteria
            let matchStage = {
                currency: { $ne: 'USD' }
            };

            if (updateSinceDate) {
                matchStage['conversionInfo.convertedAt'] = { $lt: updateSinceDate };
                console.log(`📅 Only updating records older than: ${updateSinceDate.toISOString()}`);
            }

            if (limit) {
                console.log(`🔢 Processing limit: ${limit.toLocaleString()} records`);
            }

            // Get current exchange rates
            if (this.exchangeRatesCache.size === 0) {
                await this.refreshExchangeRateCache();
            }

            // Create array of exchange rate conditions for $switch
            const switchBranches = [];
            for (const [currency, rate] of this.exchangeRatesCache.entries()) {
                if (currency !== 'USD') {
                    switchBranches.push({
                        case: { $eq: ['$currency', currency] },
                        then: rate
                    });
                }
            }

            // Count documents before update (total available)
            const totalAvailable = await this.listingsCollection.countDocuments(matchStage);
            const recordsToProcess = limit ? Math.min(limit, totalAvailable) : totalAvailable;

            console.log(`📊 Total available records: ${totalAvailable.toLocaleString()}`);
            console.log(`📊 Records to process: ${recordsToProcess.toLocaleString()}`);

            if (recordsToProcess === 0) {
                console.log('✅ No records need updating');
                return {
                    totalAvailable: totalAvailable,
                    recordsToProcess: 0,
                    updatedRecords: 0,
                    executionTimeMs: Date.now() - startTime,
                    limitApplied: Boolean(limit)
                };
            }

            // Build aggregation pipeline
            const pipeline = [
                { $match: matchStage }
            ];

            // Add limit stage if specified
            if (limit) {
                pipeline.push({ $limit: limit });
            }

            // Add processing stages
            pipeline.push(
                {
                    $addFields: {
                        // Calculate new USD price using $switch for exchange rates
                        newPriceUSD: {
                            $multiply: [
                                { $toDouble: '$priceLocal' },
                                {
                                    $switch: {
                                        branches: switchBranches,
                                        default: 1 // Fallback to 1 if currency not found
                                    }
                                }
                            ]
                        },
                        // Get the exchange rate for this currency
                        currentExchangeRate: {
                            $switch: {
                                branches: switchBranches,
                                default: 1
                            }
                        }
                    }
                },
                {
                    $addFields: {
                        // Convert to Decimal128 and round to 4 decimal places
                        priceUSD: {
                            $toDecimal: {
                                $round: ['$newPriceUSD', 4]
                            }
                        },
                        'conversionInfo.usdConversionRate': '$currentExchangeRate',
                        'conversionInfo.convertedAt': new Date(),
                        'conversionInfo.recalculatedVia': 'pure_aggregation_pipeline'
                    }
                },
                {
                    $unset: ['newPriceUSD', 'currentExchangeRate'] // Remove temporary fields
                },
                {
                    $merge: {
                        into: this.listingsCollectionName,
                        whenMatched: 'replace'
                    }
                }
            );

            // Execute the aggregation pipeline
            console.log('⚡ Executing aggregation pipeline...');
            console.log(`🔧 Pipeline stages: ${pipeline.length}`);
            if (limit) {
                console.log(`⚠️ Note: Processing limited to first ${limit.toLocaleString()} matching records`);
            }

            const aggregationResult = await this.listingsCollection.aggregate(pipeline).toArray();

            const totalExecutionTime = Date.now() - startTime;

            // Since $merge doesn't return modified count, we'll assume all matched documents were updated
            const updatedCount = recordsToProcess;

            const metrics = {
                totalAvailable: totalAvailable,
                recordsToProcess: recordsToProcess,
                updatedRecords: updatedCount,
                executionTimeMs: totalExecutionTime,
                recordsPerSecond: updatedCount > 0 ? Math.round((updatedCount / totalExecutionTime) * 1000) : 0,
                method: 'pure_aggregation_pipeline',
                limitApplied: Boolean(limit),
                limitValue: limit
            };

            // Display metrics
            console.log('\n📊 USD Price Recalculation Complete (Pure Aggregation)');
            console.log('='.repeat(70));
            console.log(`✅ Total Execution Time: ${totalExecutionTime}ms (${(totalExecutionTime / 1000).toFixed(2)}s)`);
            console.log(`📋 Total Available Records: ${totalAvailable.toLocaleString()}`);
            console.log(`📋 Records Processed: ${updatedCount.toLocaleString()}`);
            console.log(`🚀 Processing Speed: ${metrics.recordsPerSecond.toLocaleString()} records/second`);
            console.log(`⚡ Method: Pure MongoDB Aggregation Pipeline`);

            if (limit) {
                console.log(`🔢 Limit Applied: ${limit.toLocaleString()} records`);
                if (totalAvailable > limit) {
                    console.log(`⚠️ Remaining Records: ${(totalAvailable - limit).toLocaleString()} (run again to process more)`);
                }
            }

            if (updateSinceDate) {
                console.log(`📅 Date Filter Applied: Records older than ${updateSinceDate.toISOString()}`);
            }

            console.log('='.repeat(70));

            // Performance analysis
            if (metrics.recordsPerSecond < 100) {
                console.log('⚠️ Performance Warning: Consider checking database performance or network latency');
            } else if (metrics.recordsPerSecond > 1000) {
                console.log('🚀 Excellent Performance: High throughput achieved with aggregation pipeline');
            } else {
                console.log('✅ Good Performance: Efficient aggregation processing detected');
            }

            return metrics;

        } catch (error) {
            console.error('❌ Aggregation-based USD price recalculation failed:', error);
            throw error;
        }
    }

    // Check how many records will be updated before running recalculation
    async checkRecordsToUpdate(updateSinceDate = null, limit = null) {
        try {
            const startTime = Date.now();
            console.log('🔍 Checking records that would be updated...');

            // Build match criteria (same logic as recalculation)
            let matchStage = {
                currency: { $ne: 'USD' }
            };

            if (updateSinceDate) {
                matchStage['conversionInfo.convertedAt'] = { $lt: updateSinceDate };
                console.log(`📅 Date filter: Records older than ${updateSinceDate.toISOString()}`);
            }

            if (limit) {
                console.log(`🔢 Limit filter: Maximum ${limit.toLocaleString()} records`);
            }

            // Get current exchange rates for validation
            if (this.exchangeRatesCache.size === 0) {
                await this.refreshExchangeRateCache();
            }

            // Count total matching records
            const totalMatchingRecords = await this.listingsCollection.countDocuments(matchStage);
            const recordsToProcess = limit ? Math.min(limit, totalMatchingRecords) : totalMatchingRecords;

            // Get breakdown by currency
            const currencyBreakdown = await this.listingsCollection.aggregate([
                { $match: matchStage },
                {
                    $group: {
                        _id: '$currency',
                        count: { $sum: 1 },
                        avgLocalPrice: { $avg: '$priceLocal' },
                        minLocalPrice: { $min: '$priceLocal' },
                        maxLocalPrice: { $max: '$priceLocal' }
                    }
                },
                { $sort: { count: -1 } }
            ]).toArray();

            // Get date range analysis
            let dateRangeAnalysis = null;
            if (totalMatchingRecords > 0) {
                dateRangeAnalysis = await this.listingsCollection.aggregate([
                    { $match: matchStage },
                    {
                        $group: {
                            _id: null,
                            oldestRecord: { $min: '$conversionInfo.convertedAt' },
                            newestRecord: { $max: '$conversionInfo.convertedAt' },
                            avgConversionDate: { $avg: '$conversionInfo.convertedAt' }
                        }
                    }
                ]).toArray();
            }

            // Check for records missing exchange rates
            const availableCurrencies = Array.from(this.exchangeRatesCache.keys());
            const missingRatesCurrencies = currencyBreakdown
                .filter(curr => !availableCurrencies.includes(curr._id))
                .map(curr => curr._id);

            // Calculate estimated processing time based on records
            const estimatedTimeMs = Math.max(50, recordsToProcess * 0.5); // Rough estimate
            const estimatedTimeSeconds = estimatedTimeMs / 1000;

            const checkTime = Date.now() - startTime;

            // Create comprehensive report
            const report = {
                totalAvailable: totalMatchingRecords,
                recordsToProcess: recordsToProcess,
                limitApplied: Boolean(limit),
                dateFilterApplied: Boolean(updateSinceDate),
                currencyBreakdown: currencyBreakdown,
                dateRangeAnalysis: dateRangeAnalysis ? dateRangeAnalysis[0] : null,
                missingExchangeRates: missingRatesCurrencies,
                estimatedProcessingTime: {
                    milliseconds: estimatedTimeMs,
                    seconds: estimatedTimeSeconds,
                    humanReadable: this.formatDuration(estimatedTimeMs)
                },
                checkExecutionTime: checkTime,
                filters: {
                    updateSinceDate,
                    limit
                }
            };

            // Display comprehensive analysis
            this.displayUpdatePreview(report);

            return report;

        } catch (error) {
            console.error('❌ Record check failed:', error);
            throw error;
        }
    }

    // Helper function to format duration in human-readable format
    formatDuration(milliseconds) {
        if (milliseconds < 1000) {
            return `${milliseconds}ms`;
        } else if (milliseconds < 60000) {
            return `${(milliseconds / 1000).toFixed(1)}s`;
        } else if (milliseconds < 3600000) {
            return `${(milliseconds / 60000).toFixed(1)}m`;
        } else {
            return `${(milliseconds / 3600000).toFixed(1)}h`;
        }
    }

    // Display detailed update preview
    displayUpdatePreview(report) {
        console.log('\n📋 UPDATE PREVIEW - Records Analysis');
        console.log('='.repeat(80));

        // Summary section
        console.log(`📊 SUMMARY:`);
        console.log(`   Total Available Records: ${report.totalAvailable.toLocaleString()}`);
        console.log(`   Records to Process: ${report.recordsToProcess.toLocaleString()}`);

        if (report.limitApplied) {
            console.log(`   🔢 Limit Applied: ${report.filters.limit.toLocaleString()}`);
            if (report.totalAvailable > report.filters.limit) {
                console.log(`   ⚠️  Remaining: ${(report.totalAvailable - report.filters.limit).toLocaleString()} records will NOT be processed`);
            }
        }

        if (report.dateFilterApplied) {
            console.log(`   📅 Date Filter: Records older than ${report.filters.updateSinceDate.toISOString()}`);
        }

        console.log(`   ⏱️  Estimated Processing Time: ${report.estimatedProcessingTime.humanReadable}`);
        console.log(`   🔍 Check Execution Time: ${report.checkExecutionTime}ms`);

        // No records case
        if (report.recordsToProcess === 0) {
            console.log('\n✅ NO RECORDS TO UPDATE');
            console.log('   All records are already up to date or no records match the criteria.');
            console.log('='.repeat(80));
            return;
        }

        // Currency breakdown
        console.log(`\n💱 CURRENCY BREAKDOWN:`);
        console.log(`${'Currency'.padEnd(10)} | ${'Count'.padEnd(8)} | ${'Avg Price'.padEnd(15)} | ${'Price Range'.padEnd(25)}`);
        console.log('-'.repeat(70));

        report.currencyBreakdown.forEach(curr => {
            const currency = curr._id.padEnd(10);
            const count = curr.count.toString().padEnd(8);
            const avgPrice = this.formatPrice(curr.avgLocalPrice.toString(), curr._id).padEnd(15);
            const priceRange = `${this.formatPrice(curr.minLocalPrice.toString(), curr._id)} - ${this.formatPrice(curr.maxLocalPrice.toString(), curr._id)}`.padEnd(25);

            console.log(`${currency} | ${count} | ${avgPrice} | ${priceRange}`);
        });

        // Date range analysis
        if (report.dateRangeAnalysis) {
            console.log(`\n📅 DATE RANGE ANALYSIS:`);
            console.log(`   Oldest Record: ${report.dateRangeAnalysis.oldestRecord.toISOString()}`);
            console.log(`   Newest Record: ${report.dateRangeAnalysis.newestRecord.toISOString()}`);
            console.log(`   Average Date: ${new Date(report.dateRangeAnalysis.avgConversionDate).toISOString()}`);

            const ageSpan = report.dateRangeAnalysis.newestRecord - report.dateRangeAnalysis.oldestRecord;
            const ageSpanDays = Math.round(ageSpan / (24 * 60 * 60 * 1000));
            console.log(`   Date Span: ${ageSpanDays} days`);
        }

        // Exchange rate status
        console.log(`\n💹 EXCHANGE RATE STATUS:`);
        const availableRates = Array.from(this.exchangeRatesCache.keys()).filter(curr => curr !== 'USD');
        console.log(`   Available Rates: ${availableRates.join(', ')}`);

        if (report.missingExchangeRates.length > 0) {
            console.log(`   ⚠️  Missing Rates: ${report.missingExchangeRates.join(', ')}`);
            console.log(`   📝 Records with missing rates will be SKIPPED`);
        } else {
            console.log(`   ✅ All currencies have exchange rates available`);
        }

        // Performance estimation
        console.log(`\n⚡ PERFORMANCE ESTIMATION:`);
        console.log(`   Estimated Time: ${report.estimatedProcessingTime.humanReadable}`);
        console.log(`   Estimated Speed: ~${Math.round(report.recordsToProcess / Math.max(report.estimatedProcessingTime.seconds, 0.1)).toLocaleString()} records/second`);

        if (report.recordsToProcess > 10000) {
            console.log(`   💡 Large dataset detected. Consider using limit parameter for batch processing.`);
        }

        if (report.estimatedProcessingTime.seconds > 30) {
            console.log(`   ⏰ Long operation expected. Monitor progress during execution.`);
        }

        // Action recommendations
        console.log(`\n🎯 RECOMMENDATIONS:`);

        if (report.recordsToProcess === 0) {
            console.log(`   • No action needed - all records are up to date`);
        } else if (report.recordsToProcess < 1000) {
            console.log(`   • Small dataset - proceed with full update`);
            console.log(`   • Command: node price-conversion.js recalculate`);
        } else if (report.recordsToProcess < 10000) {
            console.log(`   • Medium dataset - safe to proceed with full update`);
            console.log(`   • Command: node price-conversion.js recalculate`);
        } else {
            console.log(`   • Large dataset - consider batch processing with limit`);
            console.log(`   • Suggested: node price-conversion.js recalculate "" 5000`);
            console.log(`   • Run multiple times: ${Math.ceil(report.recordsToProcess / 5000)} batches needed`);
        }

        if (report.missingExchangeRates.length > 0) {
            console.log(`   • ⚠️  Update exchange rates first: node price-conversion.js setup`);
        }

        if (report.dateFilterApplied && report.totalAvailable > report.recordsToProcess) {
            const unprocessed = report.totalAvailable - report.recordsToProcess;
            console.log(`   • 📅 ${unprocessed.toLocaleString()} newer records will remain unchanged`);
        }

        console.log('='.repeat(80));
        console.log('💡 Run the actual update with: node price-conversion.js recalculate [same parameters]');
    }

    // Enhanced version that shows sample records
    async checkRecordsToUpdateDetailed(updateSinceDate = null, limit = null, showSamples = true) {
        try {
            // Get basic report first
            const basicReport = await this.checkRecordsToUpdate(updateSinceDate, limit);

            if (basicReport.recordsToProcess === 0 || !showSamples) {
                return basicReport;
            }

            // Get sample records for preview
            let matchStage = {
                currency: { $ne: 'USD' }
            };

            if (updateSinceDate) {
                matchStage['conversionInfo.convertedAt'] = { $lt: updateSinceDate };
            }

            const sampleRecords = await this.listingsCollection
                .find(matchStage)
                .limit(Math.min(5, basicReport.recordsToProcess))
                .toArray();

            console.log(`\n📄 SAMPLE RECORDS (${sampleRecords.length} of ${basicReport.recordsToProcess}):`);
            console.log(`${'Title'.padEnd(20)} | ${'Currency'.padEnd(8)} | ${'Local Price'.padEnd(12)} | ${'Current USD'.padEnd(12)} | ${'Last Updated'.padEnd(20)}`);
            console.log('-'.repeat(85));

            sampleRecords.forEach(record => {
                const title = (record.title.length > 20 ? record.title.substring(0, 17) + '...' : record.title).padEnd(20);
                const currency = record.currency.padEnd(8);
                const localPrice = this.formatPrice(record.priceLocal.toString(), record.currency).padEnd(12);
                const currentUSD = ('$' + parseFloat(record.priceUSD.toString()).toFixed(2)).padEnd(12);
                const lastUpdated = (record.conversionInfo?.convertedAt?.toISOString().substring(0, 19) || 'N/A').padEnd(20);

                console.log(`${title} | ${currency} | ${localPrice} | ${currentUSD} | ${lastUpdated}`);
            });

            // Show what the new USD prices would be
            console.log(`\n🔄 PRICE CONVERSION PREVIEW:`);
            console.log(`${'Currency'.padEnd(8)} | ${'Old Rate'.padEnd(12)} | ${'New Rate'.padEnd(12)} | ${'Change'.padEnd(10)}`);
            console.log('-'.repeat(50));

            basicReport.currencyBreakdown.forEach(curr => {
                const currency = curr._id.padEnd(8);
                const newRate = this.exchangeRatesCache.get(curr._id) || 0;

                // Try to get old rate from a sample record
                const sampleRecord = sampleRecords.find(r => r.currency === curr._id);
                const oldRate = sampleRecord?.conversionInfo?.usdConversionRate || 0;

                const oldRateStr = oldRate.toFixed(6).padEnd(12);
                const newRateStr = newRate.toFixed(6).padEnd(12);

                let changePercent = 0;
                if (oldRate > 0) {
                    changePercent = ((newRate - oldRate) / oldRate) * 100;
                }

                const changeStr = (changePercent !== 0 ? `${changePercent > 0 ? '+' : ''}${changePercent.toFixed(2)}%` : 'N/A').padEnd(10);

                console.log(`${currency} | ${oldRateStr} | ${newRateStr} | ${changeStr}`);
            });

            basicReport.sampleRecords = sampleRecords;

            return basicReport;

        } catch (error) {
            console.error('❌ Detailed record check failed:', error);
            throw error;
        }
    }

    // Show current exchange rates (simplified)
    async showExchangeRates() {
        try {
            const rates = await this.exchangeRatesCollection.find({}).sort({ currency: 1 }).toArray();

            console.log('\n💱 Current Exchange Rates (All to USD)');
            console.log('='.repeat(70));
            console.log(`${'Currency'.padEnd(10)} | ${'To USD Rate'.padEnd(15)} | ${'Last Updated'.padEnd(20)} | ${'Change %'.padEnd(10)}`);
            console.log('-'.repeat(70));

            rates.forEach(rate => {
                const currency = rate.currency.padEnd(10);
                const rateVal = rate.toUSDRate.toString().padEnd(15);
                const updated = rate.lastUpdated.toISOString().substring(0, 19).padEnd(20);
                const change = rate.changePercent ? (rate.changePercent.toString() + '%').padEnd(10) : 'N/A'.padEnd(10);

                console.log(`${currency} | ${rateVal} | ${updated} | ${change}`);
            });
        } catch (error) {
            console.error('❌ Exchange rate display failed:', error);
        }
    }

    // Format price with currency symbol
    formatPrice(price, currency) {
        const symbols = {
            'PHP': '₱',
            'IDR': 'Rp',
            'MYR': 'RM',
            'USD': '$'
        };

        const symbol = symbols[currency] || currency;
        const numPrice = parseFloat(price);

        if (currency === 'IDR') {
            return `${symbol}${numPrice.toLocaleString('id-ID', { maximumFractionDigits: 0 })}`;
        } else {
            return `${symbol}${numPrice.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        }
    }

    // Clear all data
    async clearAllData() {
        try {
            const listingsResult = await this.listingsCollection.deleteMany({});
            const ratesResult = await this.exchangeRatesCollection.deleteMany({});
            this.exchangeRatesCache.clear();

            console.log(`✅ Cleared ${listingsResult.deletedCount} listings and ${ratesResult.deletedCount} exchange rates`);
        } catch (error) {
            console.error('❌ Clear operation failed:', error);
        }
    }
}

// CLI Command Handler
class PriceConversionCLI {
    constructor() {
        this.priceSystem = new PriceConversionSystem();
    }

    async init() {
        await this.priceSystem.connect();
        //await this.priceSystem.createIndexes();

        // Auto-load exchange rates cache on startup
        try {
            await this.priceSystem.refreshExchangeRateCache();
            if (this.priceSystem.exchangeRatesCache.size === 0) {
                console.log('💡 No exchange rates found. Use "setup" command to initialize rates.');
            }
        } catch (error) {
            console.log('💡 Exchange rates not initialized. Use "setup" command first.');
        }
    }

    isDateString(str) {
        // Check for common date patterns
        const datePatterns = [
            /^\d{4}-\d{2}-\d{2}$/,                    // 2024-01-15
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/,  // 2024-01-15T10:30:00
            /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/,  // 2024-01-15 10:30:00
            /^[A-Za-z]{3} \d{1,2} \d{4}/,            // Jan 15 2024
            /^\d{1,2}\/\d{1,2}\/\d{4}/               // 1/15/2024
        ];

        return datePatterns.some(pattern => pattern.test(str)) || !isNaN(Date.parse(str));
    }

    async cleanup() {
        await this.priceSystem.disconnect();
    }

    async handleCommand(args) {
        const command = args[2];
        let limit = null;

        try {
            switch (command) {
                case 'setup':
                    await this.priceSystem.initializeExchangeRates();
                    break;

                case 'generate':
                    const count = parseInt(args[3]) || 50;

                    // Check if exchange rates exist before generating
                    if (this.priceSystem.exchangeRatesCache.size === 0) {
                        console.log('⚠️ Exchange rates not found. Setting up exchange rates first...');
                        await this.priceSystem.initializeExchangeRates();
                    }

                    await this.priceSystem.generateListings(count);
                    break;

                case 'list':
                    const inputCurrency = args[3] || 'USD';
                    const sortOrder = args[4] === 'desc' ? -1 : 1;
                    limit = parseInt(args[5]) || 20;
                    const category = args[6];
                    const page = parseInt(args[7]) || 1;

                    if (!this.priceSystem.currencies.includes(inputCurrency)) {
                        console.log(`❌ Unsupported currency: ${inputCurrency}. Supported: ${this.priceSystem.currencies.join(', ')}`);
                        return;
                    }

                    await this.priceSystem.listByPrice(inputCurrency, sortOrder, limit, category, page);
                    break;

                case 'search':
                    const minPrice = parseFloat(args[3]);
                    const maxPrice = parseFloat(args[4]);
                    const searchCurrency = args[5] || 'USD';
                    const searchCategory = args[6];
                    const searchLimit = parseInt(args[7]) || 50;
                    const searchPage = parseInt(args[8]) || 1;

                    if (isNaN(minPrice) || isNaN(maxPrice)) {
                        console.log('❌ Please provide valid min and max price values');
                        return;
                    }

                    if (!this.priceSystem.currencies.includes(searchCurrency)) {
                        console.log(`❌ Unsupported currency: ${searchCurrency}. Supported: ${this.priceSystem.currencies.join(', ')}`);
                        return;
                    }

                    await this.priceSystem.searchByPriceRange(minPrice, maxPrice, searchCurrency, searchCategory, searchLimit, searchPage);
                    break;

                case 'analyze':
                    await this.priceSystem.analyzePriceDistribution();
                    break;

                case 'rates':
                    await this.priceSystem.showExchangeRates();
                    break;

                case 'update-rates':
                    //await this.priceSystem.updateExchangeRates();
                    await this.priceSystem.recalculateUSDPrices();
                    break;

                case 'check':
                    // Parse same parameters as recalculate but only show preview
                    let checkUpdateSinceDate = null;
                    let checkLimit = null;
                    const showDetailed = args[args.length - 1] === '--detailed' || args[args.length - 1] === '-d';

                    // Remove --detailed flag from args for processing
                    const checkArgs = showDetailed ? args.slice(0, -1) : args;

                    // Use same parameter parsing logic as recalculate
                    if (checkArgs[3] && this.isDateString(checkArgs[3])) {
                        try {
                            checkUpdateSinceDate = new Date(checkArgs[3]);
                            if (isNaN(checkUpdateSinceDate.getTime())) {
                                throw new Error('Invalid date format');
                            }
                            checkLimit = checkArgs[4] ? parseInt(checkArgs[4]) : null;
                        } catch (error) {
                            console.log(`❌ Invalid date format: ${checkArgs[3]}`);
                            console.log('💡 Use ISO format (2024-01-15T10:30:00Z) or common formats (2024-01-15, Jan 15 2024)');
                            return;
                        }
                    } else {
                        // Parse time formats
                        if (checkArgs[3] && checkArgs[3].includes('h')) {
                            const hours = parseFloat(checkArgs[3].replace('h', ''));
                            if (!isNaN(hours) && hours > 0) {
                                checkUpdateSinceDate = new Date(Date.now() - hours * 60 * 60 * 1000);
                            }
                            checkLimit = checkArgs[4] ? parseInt(checkArgs[4]) : null;
                        } else if (checkArgs[3] && checkArgs[3].includes('m')) {
                            const minutes = parseFloat(checkArgs[3].replace('m', ''));
                            if (!isNaN(minutes) && minutes > 0) {
                                checkUpdateSinceDate = new Date(Date.now() - minutes * 60 * 1000);
                            }
                            checkLimit = checkArgs[4] ? parseInt(checkArgs[4]) : null;
                        } else if (checkArgs[3] && !isNaN(parseFloat(checkArgs[3]))) {
                            const daysBack = parseFloat(checkArgs[3]);
                            if (daysBack > 0) {
                                checkUpdateSinceDate = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
                                checkLimit = checkArgs[4] ? parseInt(checkArgs[4]) : null;
                            }
                        } else {
                            checkLimit = checkArgs[3] && !isNaN(parseInt(checkArgs[3])) ? parseInt(checkArgs[3]) : null;
                        }
                    }

                    // Validate limit
                    if (checkLimit && (isNaN(checkLimit) || checkLimit <= 0)) {
                        console.log('❌ Please provide a valid positive number for limit');
                        return;
                    }

                    console.log('🔍 Analyzing records that would be updated...');
                    if (showDetailed) {
                        await this.priceSystem.checkRecordsToUpdateDetailed(checkUpdateSinceDate, checkLimit, true);
                    } else {
                        await this.priceSystem.checkRecordsToUpdate(checkUpdateSinceDate, checkLimit);
                    }
                    break;

                case 'recalculate':
                    // Parse optional parameters: [daysBack] [hoursBack] [limit] or [dateTime] [limit]
                    let updateSinceDate = null;
                    limit = null;

                    // Check if first parameter is a date string (ISO format or common formats)
                    if (args[3] && this.isDateString(args[3])) {
                        // First parameter is a date string
                        try {
                            updateSinceDate = new Date(args[3]);
                            if (isNaN(updateSinceDate.getTime())) {
                                throw new Error('Invalid date format');
                            }
                            limit = args[4] ? parseInt(args[4]) : null;
                            console.log(`📅 Using provided date: ${updateSinceDate.toISOString()}`);
                        } catch (error) {
                            console.log(`❌ Invalid date format: ${args[3]}`);
                            console.log('💡 Use ISO format (2024-01-15T10:30:00Z) or common formats (2024-01-15, Jan 15 2024)');
                            return;
                        }
                    } else {
                        // Parse as days/hours back + limit
                        const daysBack = args[3] ? parseFloat(args[3]) : null;
                        const hoursBack = args[4] && !isNaN(parseFloat(args[4])) && args[4].includes('.') ? null : parseFloat(args[4]);

                        // Determine if args[4] is hours or limit
                        if (args[4] && args[4].includes('h')) {
                            // Format like "12h" for hours
                            const hours = parseFloat(args[4].replace('h', ''));
                            if (!isNaN(hours) && hours > 0) {
                                updateSinceDate = new Date(Date.now() - hours * 60 * 60 * 1000);
                                console.log(`📅 Will update records older than ${hours} hours (${updateSinceDate.toISOString()})`);
                            }
                            limit = args[5] ? parseInt(args[5]) : null;
                        } else if (args[4] && args[4].includes('m')) {
                            // Format like "30m" for minutes
                            const minutes = parseFloat(args[4].replace('m', ''));
                            if (!isNaN(minutes) && minutes > 0) {
                                updateSinceDate = new Date(Date.now() - minutes * 60 * 1000);
                                console.log(`📅 Will update records older than ${minutes} minutes (${updateSinceDate.toISOString()})`);
                            }
                            limit = args[5] ? parseInt(args[5]) : null;
                        } else if (args[3] && args[3].includes('h')) {
                            // Format like "12h" for hours as first parameter
                            const hours = parseFloat(args[3].replace('h', ''));
                            if (!isNaN(hours) && hours > 0) {
                                updateSinceDate = new Date(Date.now() - hours * 60 * 60 * 1000);
                                console.log(`📅 Will update records older than ${hours} hours (${updateSinceDate.toISOString()})`);
                            }
                            limit = args[4] ? parseInt(args[4]) : null;
                        } else if (args[3] && args[3].includes('m')) {
                            // Format like "30m" for minutes as first parameter
                            const minutes = parseFloat(args[3].replace('m', ''));
                            if (!isNaN(minutes) && minutes > 0) {
                                updateSinceDate = new Date(Date.now() - minutes * 60 * 1000);
                                console.log(`📅 Will update records older than ${minutes} minutes (${updateSinceDate.toISOString()})`);
                            }
                            limit = args[4] ? parseInt(args[4]) : null;
                        } else if (daysBack && !isNaN(daysBack) && daysBack > 0) {
                            // Days back calculation with decimal support
                            const millisecondsBack = daysBack * 24 * 60 * 60 * 1000;
                            updateSinceDate = new Date(Date.now() - millisecondsBack);

                            if (daysBack < 1) {
                                const hoursEquivalent = daysBack * 24;
                                console.log(`📅 Will update records older than ${daysBack} days (${hoursEquivalent} hours) - ${updateSinceDate.toISOString()}`);
                            } else {
                                console.log(`📅 Will update records older than ${daysBack} days (${updateSinceDate.toISOString()})`);
                            }

                            // Check if next parameter is limit
                            limit = args[4] && !args[4].includes('h') && !args[4].includes('m') ? parseInt(args[4]) : null;
                            if (args[5] && !limit) {
                                limit = parseInt(args[5]);
                            }
                        } else {
                            // No date specified, check for limit only
                            limit = args[3] && !isNaN(parseInt(args[3])) ? parseInt(args[3]) : null;
                            if (!limit && args[4]) {
                                limit = parseInt(args[4]);
                            }
                        }
                    }

                    // Validate limit
                    if (limit && (isNaN(limit) || limit <= 0)) {
                        console.log('❌ Please provide a valid positive number for limit');
                        return;
                    }

                    // Display what will be processed
                    if (updateSinceDate && limit) {
                        console.log(`🎯 Processing: Up to ${limit.toLocaleString()} records older than ${updateSinceDate.toISOString()}`);
                    } else if (updateSinceDate) {
                        console.log(`🎯 Processing: All records older than ${updateSinceDate.toISOString()}`);
                    } else if (limit) {
                        console.log(`🎯 Processing: Up to ${limit.toLocaleString()} records (no date filter)`);
                    } else {
                        console.log(`🎯 Processing: All non-USD records (no filters)`);
                    }

                    await this.priceSystem.recalculateUSDPricesAggregation(updateSinceDate, limit);
                    break;


                case 'clear':
                    await this.priceSystem.clearAllData();
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

    showHelp() {
        console.log(`
💱 MongoDB Atlas Price Conversion System Commands

🏗️ Setup:
  node price-conversion.js setup
    Initialize exchange rates (PHP, IDR, MYR, USD - all to USD only)

📝 Data Generation:
  node price-conversion.js generate [count]
    Example: node price-conversion.js generate 100
    Default: 50 listings with mixed currencies

📋 Listing Management (WITH PAGINATION & PERFORMANCE):
  node price-conversion.js list [currency] [order] [limit] [category] [page]
    Example: node price-conversion.js list PHP asc 25 Electronics 1
    Example: node price-conversion.js list USD desc 20 "" 2
    Currency: PHP, IDR, MYR, USD (sort by this currency)
    Order: asc (low to high), desc (high to low)
    Limit: Results per page (default: 20)
    Page: Page number (default: 1)

🔍 Price Search (WITH PAGINATION & PERFORMANCE):
  node price-conversion.js search [minPrice] [maxPrice] [currency] [category] [limit] [page]
    Example: node price-conversion.js search 500 2000 PHP "" 30 1
    Example: node price-conversion.js search 10 100 USD Electronics 25 2
    Example: node price-conversion.js search 50000 200000 IDR "" 40 1
    Searches using specified currency range with pagination

📊 Analysis:
  node price-conversion.js analyze
    Shows price distribution across currencies and categories

💱 Exchange Rates:
  node price-conversion.js rates
    Display current exchange rates (all to USD)

🔄 Rate Updates:
  node price-conversion.js update-rates
    Simulate real-time rate updates (±2% fluctuation)
    Automatically recalculates all USD prices

🧹 Cleanup:
  node price-conversion.js clear
    Remove all listings and exchange rates

❓ Help:
  node price-conversion.js help

🔧 Performance Features:
  • Query execution time monitoring
  • Index usage analysis (via explain plans)
  • Document examination efficiency metrics
  • Pagination with page size control
  • Performance warnings and recommendations

🎯 Key Features:
  • Simplified exchange rates (only to USD)
  • Sort by any supported currency (PHP, IDR, MYR, USD)
  • Efficient USD-based storage with dynamic conversion
  • Multi-currency price range searches
  • Real-time rate simulation
  • Complete performance monitoring

💡 Workflow:
  1. node price-conversion.js setup           # Initialize exchange rates (REQUIRED FIRST)
  2. node price-conversion.js generate 100    # Generate listings (auto-setup if needed)
  3. node price-conversion.js list PHP asc 20 "" 1 # Sort by PHP prices, page 1
  4. node price-conversion.js search 1000 5000 PHP Electronics 25 1 # Search PHP range, 25 per page
  5. node price-conversion.js update-rates    # Simulate rate changes
  6. node price-conversion.js analyze         # View statistics

⚠️ Important: Always run 'setup' first to initialize exchange rates, or the system will auto-initialize when needed.

🌍 Currency Examples with Performance Monitoring:
  • PHP: node price-conversion.js list PHP desc 15 "" 1
  • IDR: node price-conversion.js search 100000 500000 IDR "" 30 1
  • MYR: node price-conversion.js list MYR asc 10 Books 2
  • USD: node price-conversion.js search 25 100 USD "" 20 3

📊 Performance Examples:
  • Large page: node price-conversion.js list USD asc 100 "" 1
  • Category filter: node price-conversion.js search 10 50 USD Electronics 25 1
  • Multi-page browse: node price-conversion.js list PHP asc 20 "" 3
        `);
    }
}

// Main execution
async function main() {
    const cli = new PriceConversionCLI();

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

module.exports = { PriceConversionSystem, PriceConversionCLI };