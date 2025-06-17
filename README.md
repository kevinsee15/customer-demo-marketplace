# MongoDB Atlas Demo Suite

## Quick Examples

Round Robin

```javascript
node marketplace.js search RPG round-robin 1 10 200
```

Price Conversion

```
node price-conversion.js search 10 100 USD "Electronics" 25 2

node price-conversion.js search 1000 5000 PHP "Electronics" 25 1
node price-conversion.js search 17.80 19.00 USD "Electronics" 25 1
```

Big Numbers

```javascript
node big-numbers.js search 667788990227818.3750 667788990227818.3751
```


---

## Overview

This repository contains three MongoDB Atlas demonstration applications showcasing different database features and use cases:


1. **Fair Marketplace Demo** (`marketplace.js`) - Demonstrates round-robin seller distribution algorithms
2. **Price Conversion System** (`price-conversion.js`) - Multi-currency pricing with real-time conversion
3. **Big Numbers Test** (`big-numbers.js`) - Decimal128 precision for financial calculations

## Prerequisites

* Node.js (v16 or higher)
* MongoDB Atlas cluster
* NPM or Yarn package manager

## Setup


1. **Install dependencies:**

   ```bash
   npm install
   ```
2. **Environment configuration:**

   ```bash
   # Copy the example environment file
   cp .env.example .env
   
   # Edit .env with your actual MongoDB Atlas connection details
   nano .env  # or use your preferred editor
   ```

   Update the `.env` file with your MongoDB Atlas connection string:

   ```env
   MONGODB_URI=mongodb+srv://your-username:your-password@your-cluster.mongodb.net/
   DB_NAME=customer_demo_round_robin
   COLLECTION_NAME=offers
   NODE_ENV=development
   ```

   **To get your MongoDB Atlas connection string:**
   * Log into MongoDB Atlas
   * Go to your cluster
   * Click "Connect" → "Connect your application"
   * Copy the connection string and replace `<username>` and `<password>` with your credentials
3. **Configuration:**
   All application settings are managed in `config.js` including database names, collection names, pagination settings, and performance parameters. The `.env` file overrides these defaults.

## Applications

### 1. Fair Marketplace Demo

Implements multiple seller distribution algorithms to ensure fair exposure in marketplace listings.

**Features:**

* Hash-based round-robin distribution
* True round-robin (JavaScript processing)
* Weighted random distribution
* Quota-based distribution
* Performance comparison tools

**Quick Start:**

```bash
# Initialize data
node marketplace.js seed 100 15

# Search with round-robin
node marketplace.js search RPG hash-round-robin 1 10

# Compare distribution methods
node marketplace.js compare Strategy 25

# View statistics
node marketplace.js stats RPG
```

**Available Commands:**

* `seed [offers] [sellers]` - Generate sample data
* `search [category] [method] [page] [limit]` - Search with distribution method
* `stats [category]` - View seller statistics
* `compare [category] [limit]` - Compare distribution fairness
* `debug [category]` - Debug distribution algorithms

### 2. Price Conversion System

Multi-currency marketplace with USD as the peg currency for efficient sorting and searching.

**Features:**

* Support for PHP, IDR, MYR, USD currencies
* Real-time exchange rate simulation
* Efficient USD-based indexing
* Performance monitoring with explain plans
* Pagination support
* Bulk price recalculation

**Quick Start:**

```bash
# Setup exchange rates
node price-conversion.js setup

# Generate listings
node price-conversion.js generate 100

# List by currency with pagination
node price-conversion.js list PHP asc 20 Electronics 1

# Search price ranges
node price-conversion.js search 1000 5000 PHP Electronics 25 1

# Analyze distribution
node price-conversion.js analyze
```

**Available Commands:**

* `setup` - Initialize exchange rates
* `generate [count]` - Create sample listings
* `list [currency] [order] [limit] [category] [page]` - List with sorting
* `search [min] [max] [currency] [category] [limit] [page]` - Price range search
* `rates` - Display current exchange rates
* `update-rates` - Simulate rate changes and recalculate
* `recalculate [date/days] [limit]` - Bulk USD price updates
* `check [date/days] [limit]` - Preview recalculation impact

### 3. Big Numbers Test

Demonstrates MongoDB's Decimal128 type for precise decimal arithmetic, essential for financial applications.

**Features:**

* Decimal128 precision (34 decimal digits)
* Comparison with Float64 limitations
* Precise mathematical operations
* Financial calculation accuracy
* Large number handling

**Quick Start:**

```bash
# Generate test data
node big-numbers.js insert 30

# List with precision comparison
node big-numbers.js list bigNumber asc 10

# View detailed analysis
node big-numbers.js detail big_num_1

# Search by range
node big-numbers.js search 667788990011000 667788990012000

# Analyze precision
node big-numbers.js analyze
```

**Available Commands:**

* `insert [count]` - Generate Decimal128 test data
* `list [sortBy] [order] [limit]` - Display with precision comparison
* `detail [documentId]` - Detailed view of calculations
* `search [min] [max] [category]` - Range search with Decimal128 precision
* `analyze` - Statistical analysis of big numbers
* `clear` - Remove all test data

## Project Structure

```
├── marketplace.js          # Fair marketplace demo with round-robin distribution
├── price-conversion.js     # Multi-currency pricing system
├── big-numbers.js          # Decimal128 precision testing
├── config.js              # Centralized configuration management
├── package.json           # Project dependencies and scripts
├── .env.example           # Environment variables template
├── .env                   # Your actual environment configuration (not in repo)
└── README.md              # This documentation
```

## Dependencies

The project uses the following key dependencies defined in `package.json`:

* **mongodb** (^6.3.0) - MongoDB Node.js driver
* **@faker-js/faker** (^8.4.0) - Generate realistic test data
* **dotenv** (^16.3.1) - Environment variable management

## NPM Scripts

Convenient shortcuts defined in `package.json`:

```bash
npm run seed        # Generate marketplace data
npm run search      # Search marketplace offers
npm run stats       # View seller statistics
npm run compare     # Compare distribution methods
npm run config      # Show current configuration
npm run help        # Display help information
```

## Performance Features

All applications include:

* **Index optimization** with compound indexes for efficient queries
* **Query performance monitoring** with execution time tracking
* **Explain plan analysis** to verify index usage
* **Pagination support** for large datasets
* **Aggregation pipeline optimization** with `allowDiskUse` and timeout settings
* **Batch processing capabilities** for bulk operations

## Security Notes

* Never commit `.env` files to version control
* The `.env.example` file provides a template for required environment variables
* Add `.env` to your `.gitignore` file
* Use MongoDB Atlas IP whitelisting and strong passwords
* Consider using MongoDB Atlas private endpoints for production

## Common Use Cases

### Development & Testing

```bash
# Quick development setup
cp .env.example .env
# Edit .env with your MongoDB Atlas credentials
npm install
node marketplace.js seed 50 10
node marketplace.js search RPG round-robin
```

### Performance Analysis

```bash
# Test marketplace distribution fairness
node marketplace.js compare Strategy 50

# Analyze price conversion performance
node price-conversion.js list USD desc 100 "" 1

# Test decimal precision
node big-numbers.js analyze
```

### Production Simulation

```bash
# Large dataset testing
node marketplace.js seed 1000 50
node price-conversion.js generate 500
node big-numbers.js insert 100

# Performance monitoring
node price-conversion.js search 10 100 USD "" 50 1
```

## Troubleshooting

**Connection Issues:**

* Verify your MongoDB Atlas connection string in `.env`
* Check IP whitelist settings in MongoDB Atlas
* Ensure your cluster is running and accessible

**Performance Issues:**

* Monitor index usage with the built-in explain plan analysis
* Consider increasing `maxPoolSize` in `config.js`
* Check network latency to your MongoDB Atlas cluster

**Environment Setup:**

* Ensure `.env` file exists and is properly formatted
* Verify all required environment variables are set
* Check Node.js version compatibility (requires v16+)

## Contributing


1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the `package.json` file for details.