// MongoDB Atlas Big Numbers Test with Decimal128
// This test demonstrates working with precise decimal numbers using Decimal128

// Load environment variables from .env file
require('dotenv').config();

const { MongoClient, Decimal128 } = require('mongodb');
const { faker } = require('@faker-js/faker');
const { config, validateConfig, getDatabaseName } = require('./config');

class BigNumbersTest {
    constructor() {
        this.client = null;
        this.db = null;
        this.collection = null;
        this.config = config;
        this.collectionName = 'big_numbers';

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
            this.collection = this.db.collection(this.collectionName);

            if (this.config.environment.enableDebugOutput) {
                console.log(`🔗 Connected to: ${getDatabaseName()}.${this.collectionName}`);
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

    // Create indexes for big numbers collection
    async createIndexes() {
        try {
            const indexes = [
                { key: { bigNumber: 1 }, name: 'big_number_idx' },
                { key: { category: 1 }, name: 'category_idx' },
                { key: { timestamp: -1 }, name: 'timestamp_idx' },
                { key: { bigNumber: 1, category: 1 }, name: 'compound_idx' }
            ];

            await this.collection.createIndexes(indexes);
            console.log('✅ Big numbers indexes created successfully');
        } catch (error) {
            console.error('❌ Index creation failed:', error);
        }
    }

    // Generate test data with Decimal128 big numbers
    async insertTestData(count = 20) {
        const categories = ['financial', 'scientific', 'statistical', 'measurement', 'identifier'];
        const testData = [];

        for (let i = 1; i <= count; i++) {
            // Generate various types of big numbers with exactly 4 decimal places using Decimal128
            const baseNumberStr = '667788990011223.2000';
            const variation = (Math.random() * 1000000).toFixed(4); // Random variation with 4 decimals
            const bigNumberStr = (parseFloat(baseNumberStr) + parseFloat(variation)).toFixed(4);

            // Create Decimal128 instances for precise arithmetic
            const bigNumberDecimal = Decimal128.fromString(bigNumberStr);
            const twoDecimal = Decimal128.fromString('2.0000');
            const hundredDecimal = Decimal128.fromString('100.0000');

            const document = {
                _id: `big_num_${i}`,
                bigNumber: bigNumberDecimal,
                bigNumberString: bigNumberStr,
                bigNumberFloat: parseFloat(bigNumberStr), // For comparison
                category: faker.helpers.arrayElement(categories),
                description: faker.lorem.sentence(),
                metadata: {
                    precision: 18, // Decimal128 precision
                    scale: 4, // Always 4 decimal places
                    isApproximate: faker.datatype.boolean(),
                    unit: faker.helpers.arrayElement(['bytes', 'nanoseconds', 'dollars', 'particles', 'records']),
                    dataType: 'Decimal128'
                },
                scientificNotation: parseFloat(bigNumberStr).toExponential(4),
                timestamp: new Date(),
                tags: faker.helpers.arrayElements(['large', 'precise', 'calculated', 'measured', 'estimated'], { min: 1, max: 3 }),

                // Test different number formats using Decimal128
                numberVariations: {
                    original: bigNumberDecimal,
                    originalString: bigNumberStr,
                    rounded: Decimal128.fromString(Math.round(parseFloat(bigNumberStr)).toFixed(4)),
                    floored: Decimal128.fromString(Math.floor(parseFloat(bigNumberStr)).toFixed(4)),
                    ceiled: Decimal128.fromString(Math.ceil(parseFloat(bigNumberStr)).toFixed(4)),
                    fixed4: bigNumberDecimal, // Already has 4 decimals
                    asFloat: parseFloat(bigNumberStr) // For comparison with float precision
                },

                // Mathematical operations using Decimal128 precision
                calculations: {
                    // Note: Decimal128 arithmetic in MongoDB requires aggregation pipeline
                    // For now, we'll store the string representations and do calculations in queries
                    doubledStr: (parseFloat(bigNumberStr) * 2).toFixed(4),
                    halvedStr: (parseFloat(bigNumberStr) / 2).toFixed(4),
                    percentageStr: (parseFloat(bigNumberStr) / 100).toFixed(4),
                    sqrtStr: Math.sqrt(parseFloat(bigNumberStr)).toFixed(4),
                    logStr: Math.log(parseFloat(bigNumberStr)).toFixed(4),

                    // Store as Decimal128 for precise operations
                    doubled: Decimal128.fromString((parseFloat(bigNumberStr) * 2).toFixed(4)),
                    halved: Decimal128.fromString((parseFloat(bigNumberStr) / 2).toFixed(4)),
                    percentage: Decimal128.fromString((parseFloat(bigNumberStr) / 100).toFixed(4))
                },

                // Precision comparison
                precisionComparison: {
                    decimal128: bigNumberDecimal,
                    float64: parseFloat(bigNumberStr),
                    string: bigNumberStr,
                    // Show potential precision loss with float
                    floatToString: parseFloat(bigNumberStr).toString(),
                    decimal128ToString: bigNumberDecimal.toString()
                }
            };

            testData.push(document);
        }

        try {
            // Clear existing data
            await this.collection.deleteMany({});

            // Insert new test data
            const result = await this.collection.insertMany(testData);
            console.log(`✅ Inserted ${result.insertedCount} Decimal128 big number documents (all with 4 decimal places)`);
            console.log(`📊 Using Decimal128 for precise decimal arithmetic`);

            return result;
        } catch (error) {
            console.error('❌ Data insertion failed:', error);
            throw error;
        }
    }

    // List all big numbers with Decimal128 formatting
    async listBigNumbers(sortBy = 'bigNumber', order = 1, limit = null) {
        try {
            const sortOrder = order === -1 ? -1 : 1;
            const sortOptions = {};
            sortOptions[sortBy] = sortOrder;

            let query = this.collection.find({}).sort(sortOptions);
            if (limit) {
                query = query.limit(limit);
            }

            const documents = await query.toArray();

            console.log(`\n📊 Decimal128 Big Numbers Collection (${documents.length} documents)`);
            console.log('='.repeat(140));
            console.log(`${'ID'.padEnd(12)} | ${'Decimal128 Number'.padEnd(25)} | ${'Float64 Number'.padEnd(25)} | ${'Category'.padEnd(12)} | ${'Description'.padEnd(25)}`);
            console.log('-'.repeat(140));

            documents.forEach(doc => {
                const id = doc._id.toString().padEnd(12);
                const decimal128Num = doc.bigNumber.toString().padEnd(25);
                const float64Num = doc.bigNumberFloat.toFixed(4).padEnd(25);
                const category = doc.category.padEnd(12);
                const description = (doc.description.length > 25 ?
                    doc.description.substring(0, 22) + '...' :
                    doc.description).padEnd(25);

                console.log(`${id} | ${decimal128Num} | ${float64Num} | ${category} | ${description}`);
            });

            console.log(`\n💡 Note: Decimal128 provides exact decimal representation, Float64 may have precision limits`);

            return documents;
        } catch (error) {
            console.error('❌ Failed to list big numbers:', error);
            throw error;
        }
    }

    // Get detailed view of a Decimal128 document
    async getDetailedView(documentId) {
        try {
            const document = await this.collection.findOne({ _id: documentId });

            if (!document) {
                console.log(`❌ Document with ID '${documentId}' not found`);
                return null;
            }

            console.log(`\n📋 Detailed Decimal128 View: ${documentId}`);
            console.log('='.repeat(80));
            console.log(`Decimal128 Number: ${document.bigNumber.toString()}`);
            console.log(`Float64 Number: ${document.bigNumberFloat.toFixed(4)}`);
            console.log(`String Representation: ${document.bigNumberString}`);
            console.log(`Category: ${document.category}`);
            console.log(`Description: ${document.description}`);
            console.log(`Scientific Notation: ${document.scientificNotation}`);
            console.log(`Timestamp: ${document.timestamp.toISOString()}`);
            console.log(`Tags: ${document.tags.join(', ')}`);

            console.log('\n📐 Number Variations:');
            Object.entries(document.numberVariations).forEach(([key, value]) => {
                if (value && typeof value.toString === 'function') {
                    console.log(`  ${key}: ${value.toString()}`);
                } else {
                    console.log(`  ${key}: ${value}`);
                }
            });

            console.log('\n🧮 Calculations (Decimal128):');
            Object.entries(document.calculations).forEach(([key, value]) => {
                if (value && typeof value.toString === 'function') {
                    console.log(`  ${key}: ${value.toString()}`);
                } else {
                    console.log(`  ${key}: ${value}`);
                }
            });

            console.log('\n🔍 Precision Comparison:');
            Object.entries(document.precisionComparison).forEach(([key, value]) => {
                console.log(`  ${key}: ${value}`);
            });

            console.log('\n📊 Metadata:');
            Object.entries(document.metadata).forEach(([key, value]) => {
                console.log(`  ${key}: ${value}`);
            });

            return document;
        } catch (error) {
            console.error('❌ Failed to get detailed view:', error);
            throw error;
        }
    }

    // Analyze big numbers collection
    async analyzeBigNumbers() {
        try {
            console.log('\n🔬 Big Numbers Analysis');
            console.log('='.repeat(60));

            // Basic statistics
            const stats = await this.collection.aggregate([
                {
                    $group: {
                        _id: null,
                        count: { $sum: 1 },
                        minNumber: { $min: "$bigNumber" },
                        maxNumber: { $max: "$bigNumber" },
                        avgNumber: { $avg: "$bigNumber" },
                        sumNumber: { $sum: "$bigNumber" }
                    }
                }
            ]).toArray();

            if (stats.length > 0) {
                const stat = stats[0];
                console.log(`Total Documents: ${stat.count}`);
                console.log(`Minimum: ${this.formatNumber(stat.minNumber)}`);
                console.log(`Maximum: ${this.formatNumber(stat.maxNumber)}`);
                console.log(`Average: ${this.formatNumber(stat.avgNumber)}`);
                console.log(`Sum: ${this.formatNumber(stat.sumNumber)}`);
            }

            // Category breakdown
            const categoryStats = await this.collection.aggregate([
                {
                    $group: {
                        _id: "$category",
                        count: { $sum: 1 },
                        avgNumber: { $avg: "$bigNumber" }
                    }
                },
                { $sort: { count: -1 } }
            ]).toArray();

            console.log('\n📊 By Category:');
            categoryStats.forEach(cat => {
                console.log(`  ${cat._id}: ${cat.count} documents, avg: ${cat.avgNumber.toFixed(4)}`);
            });

            // Number precision analysis
            const precisionStats = await this.collection.aggregate([
                {
                    $group: {
                        _id: "$metadata.precision",
                        count: { $sum: 1 }
                    }
                },
                { $sort: { _id: 1 } }
            ]).toArray();

            console.log('\n🎯 By Precision:');
            precisionStats.forEach(prec => {
                console.log(`  ${prec._id} digits: ${prec.count} documents`);
            });

            return { stats: stats[0], categoryStats, precisionStats };
        } catch (error) {
            console.error('❌ Analysis failed:', error);
            throw error;
        }
    }

    // Search Decimal128 numbers by range
    async searchByRange(minValue, maxValue, category = null) {
        try {
            // Convert search values to Decimal128 for accurate comparison
            const minDecimal = Decimal128.fromString(minValue.toString());
            const maxDecimal = Decimal128.fromString(maxValue.toString());

            const query = {
                bigNumber: { $gte: minDecimal, $lte: maxDecimal }
            };

            if (category) {
                query.category = category;
            }

            const results = await this.collection.find(query)
                .sort({ bigNumber: 1 })
                .toArray();

            console.log(`\n🔍 Decimal128 Search Results: ${minValue} to ${maxValue}${category ? ` (${category})` : ''}`);
            console.log('='.repeat(100));

            if (results.length === 0) {
                console.log('No documents found in this range.');
                return results;
            }

            console.log(`${'ID'.padEnd(12)} | ${'Decimal128'.padEnd(25)} | ${'Float64'.padEnd(20)} | ${'Category'.padEnd(12)}`);
            console.log('-'.repeat(100));

            results.forEach(doc => {
                console.log(`${doc._id.padEnd(12)} | ${doc.bigNumber.toString().padEnd(25)} | ${doc.bigNumberFloat.toFixed(4).padEnd(20)} | ${doc.category}`);
            });

            return results;
        } catch (error) {
            console.error('❌ Search failed:', error);
            throw error;
        }
    }

    // Utility function to format large numbers
    formatNumber(num) {
        if (typeof num !== 'number') return 'N/A';

        if (num >= 1e12) {
            return (num / 1e12).toFixed(2) + 'T';
        } else if (num >= 1e9) {
            return (num / 1e9).toFixed(2) + 'B';
        } else if (num >= 1e6) {
            return (num / 1e6).toFixed(2) + 'M';
        } else if (num >= 1e3) {
            return (num / 1e3).toFixed(2) + 'K';
        } else {
            return num.toLocaleString();
        }
    }

    // Clean up collection
    async clearCollection() {
        try {
            const result = await this.collection.deleteMany({});
            console.log(`✅ Cleared ${result.deletedCount} documents from collection`);
            return result;
        } catch (error) {
            console.error('❌ Clear failed:', error);
            throw error;
        }
    }
}

// CLI Command Handler
class BigNumbersCLI {
    constructor() {
        this.bigNumbers = new BigNumbersTest();
    }

    async init() {
        await this.bigNumbers.connect();
        await this.bigNumbers.createIndexes();
    }

    async cleanup() {
        await this.bigNumbers.disconnect();
    }

    async handleCommand(args) {
        const command = args[2];

        try {
            switch (command) {
                case 'insert':
                    const count = parseInt(args[3]) || 20;
                    await this.bigNumbers.insertTestData(count);
                    break;

                case 'list':
                    const sortBy = args[3] || 'bigNumber';
                    const order = args[4] === 'desc' ? -1 : 1;
                    const limit = args[5] ? parseInt(args[5]) : null;
                    await this.bigNumbers.listBigNumbers(sortBy, order, limit);
                    break;

                case 'detail':
                    const docId = args[3];
                    if (!docId) {
                        console.log('❌ Please provide a document ID');
                        return;
                    }
                    await this.bigNumbers.getDetailedView(docId);
                    break;

                case 'analyze':
                    await this.bigNumbers.analyzeBigNumbers();
                    break;

                case 'search':
                    const minVal = args[3];
                    const maxVal = args[4];
                    const category = args[5];

                    if (isNaN(minVal) || isNaN(maxVal)) {
                        console.log('❌ Please provide valid min and max values');
                        return;
                    }

                    await this.bigNumbers.searchByRange(minVal, maxVal, category);
                    break;

                case 'clear':
                    await this.bigNumbers.clearCollection();
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
🔢 MongoDB Atlas Decimal128 Big Numbers Test Commands

📝 Data Management:
  node big-numbers.js insert [count]
    Example: node big-numbers.js insert 30
    Default: 20 documents with Decimal128 precision

📋 List & View:
  node big-numbers.js list [sortBy] [order] [limit]
    Example: node big-numbers.js list bigNumber desc 10
    SortBy: bigNumber, category, timestamp
    Order: asc, desc
    Shows both Decimal128 and Float64 for comparison

🔍 Detail View:
  node big-numbers.js detail [documentId]
    Example: node big-numbers.js detail big_num_1
    Shows precision comparison between Decimal128 and Float64

🔬 Analysis:
  node big-numbers.js analyze
    Shows Decimal128 vs Float64 statistics and precision analysis

🔍 Search:
  node big-numbers.js search [minValue] [maxValue] [category]
    Example: node big-numbers.js search 667788990011000 667788990012000
    Example: node big-numbers.js search 667788990011000 667788990012000 financial
    Uses Decimal128 for precise range queries

🧹 Cleanup:
  node big-numbers.js clear
    Removes all documents from collection

❓ Help:
  node big-numbers.js help

🔧 Environment Setup:
  Uses the same .env configuration as the marketplace demo
  Collection: big_numbers
  Database: marketplace_demo (or your configured DB_NAME)

💡 Decimal128 Benefits:
  - Exact decimal representation (no floating-point errors)
  - Up to 34 decimal digits of precision
  - Perfect for financial calculations
  - Native MongoDB support for arithmetic operations
        `);
    }
}

// Main execution
async function main() {
    const cli = new BigNumbersCLI();

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

module.exports = { BigNumbersTest, BigNumbersCLI };