import express from 'express';
import cors from 'cors';
import bodyParser from 'body-parser';
import mysql from 'mysql2/promise';

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// MySQL Database Configuration - ENABLE MULTIPLE STATEMENTS
const dbConfig = {
  host: 'localhost',
  user: 'root', 
  password: 'selvisql@24',
  database: 'query_builder_db',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  charset: 'utf8mb4',
  multipleStatements: true
};

// MySQL connection pool
const mysqlPool = mysql.createPool(dbConfig);

// Initialize MySQL database with sample data
async function initializeMySQL() {
  let connection;
  try {
    // First, test connection without specifying database
    const tempConnection = await mysql.createConnection({
      host: dbConfig.host,
      user: dbConfig.user, 
      password: dbConfig.password
    });
    
    console.log('🔧 Initializing MySQL database...');

    // Create database if it doesn't exist
    await tempConnection.query(`CREATE DATABASE IF NOT EXISTS \`${dbConfig.database}\``);
    await tempConnection.end();

    // Now use the pool with the database
    connection = await mysqlPool.getConnection();

    // Create tables
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INT,
        city VARCHAR(50),
        salary DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await connection.execute(`
      CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        price DECIMAL(10, 2),
        category VARCHAR(50),
        in_stock BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await connection.execute(`
      CREATE TABLE IF NOT EXISTS orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        product_id INT,
        quantity INT,
        order_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await connection.execute(`
      CREATE TABLE IF NOT EXISTS employees (
        id INT AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        department VARCHAR(50),
        salary DECIMAL(10, 2),
        hire_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Check if sample data already exists
    const [userCount] = await connection.execute('SELECT COUNT(*) as count FROM users');
    
    if (userCount[0].count === 0) {
      console.log('📥 Inserting sample data into MySQL...');
      
      // Insert sample users
      await connection.execute(`
        INSERT INTO users (name, email, age, city, salary) VALUES 
        ('John Doe', 'john@example.com', 30, 'New York', 50000.00),
        ('Jane Smith', 'jane@example.com', 25, 'Los Angeles', 60000.00),
        ('Bob Johnson', 'bob@example.com', 35, 'Chicago', 70000.00),
        ('Alice Brown', 'alice@example.com', 28, 'New York', 55000.00),
        ('Charlie Wilson', 'charlie@example.com', 32, 'Miami', 65000.00)
      `);

      // Insert sample products
      await connection.execute(`
        INSERT INTO products (name, price, category, in_stock) VALUES 
        ('Laptop', 999.99, 'Electronics', true),
        ('Smartphone', 699.99, 'Electronics', true),
        ('Desk Chair', 199.99, 'Furniture', true),
        ('Monitor', 299.99, 'Electronics', false),
        ('Keyboard', 79.99, 'Electronics', true)
      `);

      // Insert sample orders
      await connection.execute(`
        INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES 
        (1, 1, 1, '2024-01-15'),
        (1, 2, 1, '2024-01-16'),
        (2, 3, 2, '2024-01-17'),
        (3, 4, 1, '2024-01-18'),
        (4, 5, 3, '2024-01-19')
      `);

      // Insert sample employees
      await connection.execute(`
        INSERT INTO employees (first_name, last_name, department, salary, hire_date) VALUES 
        ('Michael', 'Scott', 'Management', 80000.00, '2020-01-15'),
        ('Pam', 'Beesly', 'Admin', 45000.00, '2021-03-20'),
        ('Jim', 'Halpert', 'Sales', 55000.00, '2019-11-10'),
        ('Dwight', 'Schrute', 'Sales', 52000.00, '2018-07-05'),
        ('Angela', 'Martin', 'Accounting', 48000.00, '2022-02-14')
      `);

      console.log('✅ MySQL sample data inserted successfully');
    } else {
      console.log('✅ MySQL sample data already exists');
    }

    console.log('🎉 MySQL database initialized successfully!');
    
  } catch (error) {
    console.error('❌ MySQL initialization error:', error);
    throw error;
  } finally {
    if (connection) connection.release();
  }
}

// Execute SQL Query - UPDATED TO ADD SEMICOLONS BETWEEN STATEMENTS
app.post('/api/sql/execute', async (req, res) => {
  const { query } = req.body;
  
  if (!query) {
    return res.status(400).json({ 
      success: false,
      error: 'No query provided',
      type: 'SQL'
    });
  }

  let connection;
  try {
    connection = await mysqlPool.getConnection();
    
    // Clean the query and ensure semicolons between statements
    let cleanQuery = query.trim();
    
    // If multiple lines without semicolons, add them
    if (cleanQuery.includes('\n') && !cleanQuery.includes(';')) {
      cleanQuery = cleanQuery.split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0)
        .join(';\n') + ';';
    }
    
    console.log('🔍 Executing SQL Query:', cleanQuery);
    
    // Use query() to support multiple statements
    const [results, fields] = await connection.query(cleanQuery);
    
    // Format results for table display
    let columns = [];
    let data = [];
    
    if (Array.isArray(results) && results.length > 0) {
      // Handle multiple result sets - get the last SELECT result
      for (let i = results.length - 1; i >= 0; i--) {
        const result = results[i];
        if (Array.isArray(result) && result.length > 0) {
          data = result;
          columns = Object.keys(result[0]);
          break;
        }
      }
      
      // If no array results found, check for object results (INSERT/UPDATE/DELETE)
      if (data.length === 0) {
        const lastResult = results[results.length - 1];
        if (lastResult && typeof lastResult === 'object' && lastResult.affectedRows !== undefined) {
          data = [{
            'Status': 'Query executed successfully', 
            'Affected Rows': lastResult.affectedRows,
            'Insert ID': lastResult.insertId || 'N/A'
          }];
          columns = Object.keys(data[0]);
        }
      }
    }
    
    // Fallback if no data found
    if (data.length === 0) {
      data = [{'Status': 'Query executed successfully'}];
      columns = ['Status'];
    }

    const formattedResults = {
      success: true,
      data: data,
      columns: columns,
      count: data.length,
      type: 'SQL',
      message: getSuccessMessage(cleanQuery, results)
    };
    
    console.log('✅ Query executed successfully');
    res.json(formattedResults);
    
  } catch (error) {
    console.error('❌ SQL Execution Error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message,
      type: 'SQL',
      message: 'Query execution failed. Please check your syntax.'
    });
  } finally {
    if (connection) connection.release();
  }
});

// Helper function to generate success messages
function getSuccessMessage(query, results) {
  const lowerQuery = query.toLowerCase().trim();
  
  if (lowerQuery.startsWith('select')) {
    let rowCount = 0;
    if (Array.isArray(results)) {
      // Find the last array result (SELECT)
      for (let i = results.length - 1; i >= 0; i--) {
        if (Array.isArray(results[i])) {
          rowCount = results[i].length;
          break;
        }
      }
    }
    return `Query executed successfully. ${rowCount} row(s) returned.`;
  } else if (lowerQuery.startsWith('insert')) {
    const affectedRows = Array.isArray(results) ? results[results.length - 1]?.affectedRows : results?.affectedRows;
    return `Data inserted successfully. ${affectedRows || 0} row(s) affected.`;
  } else if (lowerQuery.startsWith('update')) {
    const affectedRows = Array.isArray(results) ? results[results.length - 1]?.affectedRows : results?.affectedRows;
    return `Data updated successfully. ${affectedRows || 0} row(s) affected.`;
  } else if (lowerQuery.startsWith('delete')) {
    const affectedRows = Array.isArray(results) ? results[results.length - 1]?.affectedRows : results?.affectedRows;
    return `Data deleted successfully. ${affectedRows || 0} row(s) affected.`;
  } else if (lowerQuery.startsWith('create')) {
    return 'Table created successfully.';
  } else {
    return 'Query executed successfully.';
  }
}

// Enhanced NoSQL query handlers with MongoDB-like operators
async function handleNoSQLFind(connection, collectionName, argsString) {
  try {
    let whereClause = '1=1';
    const queryParams = [];
    
    if (argsString.trim() !== '{}') {
      try {
        const queryObj = parseJSON(argsString);
        const conditions = [];
        
        // Process query object recursively
        const processCondition = (obj, parentKey = '') => {
          Object.keys(obj).forEach(key => {
            const value = obj[key];
            
            if (key.startsWith('$')) {
              // Handle operators
              switch (key) {
                case '$gt':
                  conditions.push(`${parentKey} > ?`);
                  queryParams.push(value);
                  break;
                case '$gte':
                  conditions.push(`${parentKey} >= ?`);
                  queryParams.push(value);
                  break;
                case '$lt':
                  conditions.push(`${parentKey} < ?`);
                  queryParams.push(value);
                  break;
                case '$lte':
                  conditions.push(`${parentKey} <= ?`);
                  queryParams.push(value);
                  break;
                case '$ne':
                  conditions.push(`${parentKey} != ?`);
                  queryParams.push(value);
                  break;
                case '$in':
                  if (Array.isArray(value)) {
                    conditions.push(`${parentKey} IN (${value.map(() => '?').join(', ')})`);
                    queryParams.push(...value);
                  }
                  break;
                case '$nin':
                  if (Array.isArray(value)) {
                    conditions.push(`${parentKey} NOT IN (${value.map(() => '?').join(', ')})`);
                    queryParams.push(...value);
                  }
                  break;
                case '$and':
                  if (Array.isArray(value)) {
                    const andConditions = value.map(cond => {
                      const subConditions = [];
                      const subParams = [];
                      Object.keys(cond).forEach(subKey => {
                        subConditions.push(`${subKey} = ?`);
                        subParams.push(cond[subKey]);
                      });
                      queryParams.push(...subParams);
                      return `(${subConditions.join(' AND ')})`;
                    });
                    conditions.push(`(${andConditions.join(' AND ')})`);
                  }
                  break;
                case '$or':
                  if (Array.isArray(value)) {
                    const orConditions = value.map(cond => {
                      const subConditions = [];
                      const subParams = [];
                      Object.keys(cond).forEach(subKey => {
                        subConditions.push(`${subKey} = ?`);
                        subParams.push(cond[subKey]);
                      });
                      queryParams.push(...subParams);
                      return `(${subConditions.join(' AND ')})`;
                    });
                    conditions.push(`(${orConditions.join(' OR ')})`);
                  }
                  break;
                case '$exists':
                  if (value === true) {
                    conditions.push(`${parentKey} IS NOT NULL`);
                  } else {
                    conditions.push(`${parentKey} IS NULL`);
                  }
                  break;
              }
            } else if (typeof value === 'object' && value !== null) {
              // Nested object with operators
              processCondition(value, key);
            } else {
              // Simple equality
              conditions.push(`${key} = ?`);
              queryParams.push(value);
            }
          });
        };
        
        processCondition(queryObj);
        
        if (conditions.length > 0) {
          whereClause = conditions.join(' AND ');
        }
      } catch (error) {
        console.warn('Query parsing failed, returning all documents:', error);
      }
    }
    
    const sql = `SELECT * FROM ${collectionName} WHERE ${whereClause}`;
    const [rows] = await connection.execute(sql, queryParams);
    
    return {
      data: rows,
      count: rows.length
    };
  } catch (error) {
    throw new Error(`NoSQL find error: ${error.message}`);
  }
}

// Execute NoSQL Query (Enhanced with MongoDB-like operations)
app.post('/api/nosql/execute', async (req, res) => {
  const { query } = req.body;
  
  if (!query) {
    return res.status(400).json({ 
      success: false,
      error: 'No query provided',
      type: 'NoSQL'
    });
  }

  let connection;
  try {
    connection = await mysqlPool.getConnection();
    
    console.log('🔍 Executing NoSQL Query:', query);
    
    // Parse the NoSQL-like query with enhanced pattern matching
    let results;
    
    // Enhanced pattern matching for different NoSQL operations
    if (query.includes('.find(') && query.includes('.sort(')) {
      // find with sort
      const match = query.match(/db\.(\w+)\.find\((.*)\)\.sort\((.*)\)/);
      if (match) {
        const [, collectionName, findArgs, sortArgs] = match;
        const findResult = await handleNoSQLFind(connection, collectionName, findArgs);
        // Apply sorting logic here
        results = findResult;
      }
    } else if (query.includes('.find(') && query.includes('.limit(')) {
      // find with limit
      const match = query.match(/db\.(\w+)\.find\((.*)\)\.limit\((\d+)\)/);
      if (match) {
        const [, collectionName, findArgs, limit] = match;
        const findResult = await handleNoSQLFind(connection, collectionName, findArgs);
        results = {
          data: findResult.data.slice(0, parseInt(limit)),
          count: Math.min(findResult.data.length, parseInt(limit))
        };
      }
    } else if (query.includes('.find(')) {
      // Basic find
      const match = query.match(/db\.(\w+)\.find\((.*)\)/);
      if (match) {
        const [, collectionName, argsString] = match;
        results = await handleNoSQLFind(connection, collectionName, argsString);
      }
    } else if (query.includes('.findOne(')) {
      // findOne
      const match = query.match(/db\.(\w+)\.findOne\((.*)\)/);
      if (match) {
        const [, collectionName, argsString] = match;
        const findResult = await handleNoSQLFind(connection, collectionName, argsString);
        results = {
          data: findResult.data.length > 0 ? [findResult.data[0]] : [],
          count: findResult.data.length > 0 ? 1 : 0
        };
      }
    } else if (query.includes('.count(')) {
      // count
      const match = query.match(/db\.(\w+)\.find\((.*)\)\.count\(\)/);
      if (match) {
        const [, collectionName, argsString] = match;
        const findResult = await handleNoSQLFind(connection, collectionName, argsString);
        results = {
          data: [{ count: findResult.count }],
          count: 1
        };
      } else {
        const simpleMatch = query.match(/db\.(\w+)\.count\(\)/);
        if (simpleMatch) {
          const [, collectionName] = simpleMatch;
          const [rows] = await connection.execute(`SELECT COUNT(*) as count FROM ${collectionName}`);
          results = {
            data: [{ count: rows[0].count }],
            count: 1
          };
        }
      }
    } else if (query.includes('.distinct(')) {
      // distinct
      const match = query.match(/db\.(\w+)\.distinct\((['"])(.*?)\1\)/);
      if (match) {
        const [, collectionName, , field] = match;
        const [rows] = await connection.execute(`SELECT DISTINCT ${field} FROM ${collectionName}`);
        results = {
          data: rows,
          count: rows.length
        };
      }
    } else if (query.includes('.insert(')) {
      // insert
      const match = query.match(/db\.(\w+)\.insert\((.*)\)/);
      if (match) {
        const [, collectionName, argsString] = match;
        const document = parseJSON(argsString);
        const columns = Object.keys(document).join(', ');
        const values = Object.values(document);
        const placeholders = values.map(() => '?').join(', ');
        
        const sql = `INSERT INTO ${collectionName} (${columns}) VALUES (${placeholders})`;
        const [result] = await connection.execute(sql, values);
        
        results = {
          data: [{ insertedId: result.insertId, operation: 'insert' }],
          count: 1
        };
      }
    } else if (query.includes('.update(')) {
      // update
      results = {
        data: [{ matchedCount: 1, modifiedCount: 1, message: 'Update operation simulated' }],
        count: 1
      };
    } else if (query.includes('.remove(')) {
      // remove
      results = {
        data: [{ deletedCount: 0, message: 'Remove operation disabled in demo mode' }],
        count: 1
      };
    } else if (query.includes('.aggregate(')) {
      // aggregate
      const match = query.match(/db\.(\w+)\.aggregate\((.*)\)/);
      if (match) {
        const [, collectionName, argsString] = match;
        // Simple aggregation simulation
        const [rows] = await connection.execute(`SELECT * FROM ${collectionName} LIMIT 10`);
        results = {
          data: rows,
          count: rows.length
        };
      }
    } else {
      throw new Error('Unsupported NoSQL operation format');
    }

    if (!results) {
      throw new Error('No results generated from query');
    }

    res.json({
      success: true,
      data: results.data || results,
      count: results.count || (Array.isArray(results) ? results.length : 1),
      type: 'NoSQL',
      message: `NoSQL operation executed successfully. Returned ${results.count || 0} document(s).`
    });

  } catch (error) {
    console.error('❌ NoSQL Execution Error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message,
      type: 'NoSQL',
      message: 'NoSQL operation failed. Please check your syntax.'
    });
  } finally {
    if (connection) connection.release();
  }
});

// Helper function to parse JSON with error handling
function parseJSON(str) {
  try {
    // Remove any trailing semicolons and trim
    let cleanStr = str.trim().replace(/;+$/, '');
    
    // Fix common JSON issues
    const fixedStr = cleanStr
      .replace(/(\w+):/g, '"$1":') // Wrap unquoted keys in double quotes
      .replace(/'/g, '"') // Replace single quotes with double quotes
      .replace(/,\s*}/g, '}') // Remove trailing commas in objects
      .replace(/,\s*]/g, ']') // Remove trailing commas in arrays
      .replace(/(\w+)\s*:\s*([^"{}\[\],\s]+)(?=\s*[,}])/g, '"$1": "$2"'); // Wrap unquoted string values
    
    return JSON.parse(fixedStr);
  } catch (error) {
    throw new Error(`Invalid JSON: ${str}. Error: ${error.message}`);
  }
}

// Health check endpoint
app.get('/api/health', async (req, res) => {
  try {
    const connection = await mysqlPool.getConnection();
    await connection.execute('SELECT 1');
    connection.release();

    res.json({ 
      status: 'healthy', 
      database: 'MySQL',
      connection: 'connected',
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({ 
      status: 'unhealthy', 
      database: 'MySQL',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Get database schema information
app.get('/api/schema', async (req, res) => {
  let connection;
  try {
    connection = await mysqlPool.getConnection();
    const [tables] = await connection.execute('SHOW TABLES');
    const schema = [];

    for (const table of tables) {
      const tableName = table[`Tables_in_${dbConfig.database}`];
      const [columns] = await connection.execute(`DESCRIBE ${tableName}`);
      schema.push({
        table: tableName,
        columns: columns.map(col => ({
          name: col.Field,
          type: col.Type,
          nullable: col.Null,
          key: col.Key,
          default: col.Default
        }))
      });
    }

    res.json(schema);
  } catch (error) {
    res.status(500).json({ error: error.message });
  } finally {
    if (connection) connection.release();
  }
});

// Get sample queries
app.get('/api/sample-queries', (req, res) => {
  const sampleQueries = {
    sql: [
      "SELECT * FROM users;",
      "SELECT name, email FROM users WHERE age > 25;",
      "SELECT * FROM products WHERE category = 'Electronics';",
      "SELECT u.name, p.name as product_name, o.quantity FROM orders o JOIN users u ON o.user_id = u.id JOIN products p ON o.product_id = p.id;",
      "SELECT city, COUNT(*) as user_count FROM users GROUP BY city;",
      "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department;"
    ],
    nosql: [
      "db.users.find({});",
      "db.users.find({age: {$gt: 25}});",
      "db.users.find({$or: [{age: {$gt: 30}}, {city: 'New York'}]});",
      "db.users.find({age: {$in: [25, 30, 35]}});",
      "db.users.find({email: {$exists: true}});",
      "db.users.find().limit(5);",
      "db.users.find().count();",
      "db.users.distinct('city');",
      "db.users.findOne({name: 'John Doe'});"
    ]
  };
  
  res.json(sampleQueries);
});

// Test endpoint
app.get('/api/test', async (req, res) => {
  let connection;
  try {
    connection = await mysqlPool.getConnection();
    const [results] = await connection.execute('SELECT * FROM users LIMIT 2');
    res.json({
      success: true,
      message: 'Database connection successful!',
      data: results
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  } finally {
    if (connection) connection.release();
  }
});

// Initialize and start server
async function startServer() {
  try {
    console.log('🚀 Starting SQL & NoSQL Query Builder Server...');
    
    // Test database connection first
    const testConnection = await mysql.createConnection({
      host: dbConfig.host,
      user: dbConfig.user,
      password: dbConfig.password
    });
    await testConnection.execute('SELECT 1');
    testConnection.end();
    console.log('✅ MySQL connection successful');
    
    // Initialize database with sample data
    await initializeMySQL();
    
    app.listen(PORT, () => {
      console.log(`🎉 Server running on port ${PORT}`);
      console.log(`📊 Health check: http://localhost:${PORT}/api/health`);
      console.log(`🧪 Test connection: http://localhost:${PORT}/api/test`);
      console.log(`🗄️  SQL endpoint: http://localhost:${PORT}/api/sql/execute`);
      console.log(`📄 NoSQL endpoint: http://localhost:${PORT}/api/nosql/execute`);
      console.log(`📋 Schema info: http://localhost:${PORT}/api/schema`);
      console.log(`💡 Sample queries: http://localhost:${PORT}/api/sample-queries`);
      console.log('\n✨ Your SQL & NoSQL Query Builder is ready!');
      console.log('👉 Enhanced NoSQL support with MongoDB-like operators added!');
    });
  } catch (error) {
    console.error('❌ Failed to start server:', error.message);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down server gracefully...');
  await mysqlPool.end();
  console.log('✅ MySQL connections closed');
  process.exit(0);
});

startServer().catch(console.error);