import { Inject, Injectable, OnModuleInit, OnModuleDestroy, Logger } from "@nestjs/common";
import { DataSource, Table, TableColumn } from "typeorm";

@Injectable()
export class SQLDbService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(SQLDbService.name);

    constructor(
        @Inject('POSTGRES_CONNECTION') private postgresDataSource: DataSource,
        // @Inject('MYSQL_CONNECTION') private mysqlDataSource: DataSource,
        // @Inject('ORACLE_CONNECTION') private oracleDataSource: DataSource,
    ) {
        this.logger.log('SQLDbService initialized with base connections');
    }

    // Cache to store data sources with timestamps and connection info
    private dataSourceCache: Record<string, {
        dataSource: DataSource,
        createdAt: number,
        lastUsed: number,
        connectionsOpened: number,
        queriesExecuted: number
    }> = {};

    // Configuration for cache management
    private readonly MAX_CACHE_SIZE = 20; // Maximum number of cached connections
    private readonly CACHE_TTL = 10 * 60 * 1000; // 10 minutes before connection cleanup
    private readonly CLEANUP_INTERVAL = 60 * 60 * 1000; // Run cleanup every 1 hr
    private cleanupInterval: NodeJS.Timeout | null = null;

    async onModuleInit() {
        // Start the cleanup interval when the service initializes
        this.logger.log(`Initializing connection cleanup interval (runs every ${this.CLEANUP_INTERVAL / 60000} minutes)`);
        this.cleanupInterval = setInterval(() => this.cleanupOldConnections(), this.CLEANUP_INTERVAL);
        
        // Log cache settings
        this.logger.log(`Connection cache settings: MAX_SIZE=${this.MAX_CACHE_SIZE}, TTL=${this.CACHE_TTL / 60000} minutes`);
    }

    async onModuleDestroy() {
        // Clear the cleanup interval when the module is destroyed
        this.logger.log('Module being destroyed, cleaning up resources...');
        
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
            this.logger.log('Cleanup interval cleared');
        }
        
        // Close all cached connections
        const connectionCount = Object.keys(this.dataSourceCache).length;
        this.logger.log(`Closing all ${connectionCount} cached connections...`);
        
        for (const key in this.dataSourceCache) {
            try {
                const cache = this.dataSourceCache[key];
                await cache.dataSource.destroy();
                this.logger.log(`Connection ${key} closed successfully`);
            } catch (error) {
                this.logger.error(`Error destroying connection ${key}: ${error.message}`);
            }
        }
        
        this.logger.log('All connections closed, module cleanup complete');
    }

    private async cleanupOldConnections() {
        this.logger.log('Running connection cache cleanup...');
        
        const now = Date.now();
        const keysToRemove: string[] = [];
        let totalConnections = Object.keys(this.dataSourceCache).length;
        let idleConnections = 0;
        
        // Find connections that haven't been used for a while
        for (const key in this.dataSourceCache) {
            const cache = this.dataSourceCache[key];
            
            // Skip base connections (the ones without a specific database name)
            if (key.endsWith('_default')) {
                continue;
            }
            
            const idleTime = now - cache.lastUsed;
            
            // Log connection status
            this.logger.verbose(
                `Connection ${key}: Idle for ${Math.floor(idleTime / 60000)} min ${Math.floor((idleTime % 60000) / 1000)} sec, ` +
                `Created: ${new Date(cache.createdAt).toISOString()}, ` +
                `Queries: ${cache.queriesExecuted}`
            );
            
            // Check if the connection has been idle for too long
            if (idleTime > this.CACHE_TTL) {
                keysToRemove.push(key);
                idleConnections++;
            }
        }
        
        // Remove and destroy old connections
        for (const key of keysToRemove) {
            this.logger.log(`Closing idle connection: ${key} (idle for ${Math.floor((now - this.dataSourceCache[key].lastUsed) / 60000)} minutes)`);
            try {
                await this.dataSourceCache[key].dataSource.destroy();
                delete this.dataSourceCache[key];
            } catch (error) {
                this.logger.error(`Error destroying connection ${key}: ${error.message}`);
            }
        }
        
        this.logger.log(
            `Cleanup completed. Removed ${keysToRemove.length} connections. ` +
            `Remaining: ${Object.keys(this.dataSourceCache).length}/${totalConnections}`
        );
        
        // Log pool status
        this.logger.log(`Connection pool statistics: Total=${totalConnections}, Idle=${idleConnections}, Active=${totalConnections - idleConnections}`);
        
        return {
            totalConnections,
            idleConnections,
            removedConnections: keysToRemove.length,
            remainingConnections: Object.keys(this.dataSourceCache).length
        };
    }

    private async enforceMaxCacheSize() {
        const keys = Object.keys(this.dataSourceCache);
        
        // Skip if we're under the limit
        if (keys.length <= this.MAX_CACHE_SIZE) {
            this.logger.debug(`Cache size ${keys.length} is within limit of ${this.MAX_CACHE_SIZE}, no cleanup needed`);
            return;
        }
        
        this.logger.log(`Cache size limit exceeded: ${keys.length}/${this.MAX_CACHE_SIZE}, removing oldest connections`);
        
        // Sort by last used time (oldest first)
        const sortedKeys = keys
            .filter(key => !key.endsWith('_default')) // Skip base connections
            .sort((a, b) => this.dataSourceCache[a].lastUsed - this.dataSourceCache[b].lastUsed);
        
        // Remove connections until we're under the limit
        const keysToRemove = sortedKeys.slice(0, keys.length - this.MAX_CACHE_SIZE);
        
        for (const key of keysToRemove) {
            this.logger.log(`Removing connection due to cache size limit: ${key} (Last used: ${new Date(this.dataSourceCache[key].lastUsed).toISOString()})`);
            try {
                await this.dataSourceCache[key].dataSource.destroy();
                delete this.dataSourceCache[key];
            } catch (error) {
                this.logger.error(`Error destroying connection ${key}: ${error.message}`);
            }
        }
        
        this.logger.log(`Cache size enforcement complete. Removed ${keysToRemove.length} connections. New size: ${Object.keys(this.dataSourceCache).length}`);
    }

    private async getDataSource(dbType: string, dbName?: string): Promise<DataSource> {
        const key = `${dbType.toLowerCase()}_${dbName || 'default'}`;
        const now = Date.now();
        
        // Return from cache if available
        if (this.dataSourceCache[key]) {
            // Update last used timestamp
            this.dataSourceCache[key].lastUsed = now;
            this.dataSourceCache[key].queriesExecuted++;
            
            const cacheAge = Math.floor((now - this.dataSourceCache[key].createdAt) / 1000);
            this.logger.log(`Reusing existing connection: ${key} (age: ${cacheAge}s, queries: ${this.dataSourceCache[key].queriesExecuted})`);
            
            return this.dataSourceCache[key].dataSource;
        }

        this.logger.log(`Creating new connection for ${dbType}${dbName ? ` with database ${dbName}` : ''}`);

        let baseDataSource: DataSource;
        switch (dbType.toLowerCase()) {
            case 'postgres':
                baseDataSource = this.postgresDataSource;
                this.logger.log('Using PostgreSQL base connection');
                break;
            // case 'mysql':
            //     baseDataSource = this.mysqlDataSource;
            //     this.logger.log('Using MySQL base connection');
            //     break;
            // case 'oracle':
            //     baseDataSource = this.oracleDataSource;
            //     this.logger.log('Using Oracle base connection');
            //     break;
            default:
                const errorMsg = `Unsupported database type: ${dbType}`;
                this.logger.error(errorMsg);
                throw new Error(errorMsg);
        }

        if (dbName) {
            // Before creating a new connection, enforce the cache size limit
            await this.enforceMaxCacheSize();
            
            const newDataSourceOptions: any = {
                ...baseDataSource.options,
                database: typeof dbName === 'string' ? dbName : new TextDecoder().decode(dbName as Uint8Array),
                // Configure connection pooling
                extra: {
                    ...((baseDataSource.options as any).extra || {}),
                    max: 10,         // Maximum connections in pool
                    min: 1,          // Minimum connections
                    idleTimeoutMillis: 30000, // Close idle connections after 30s
                    acquireTimeoutMillis: 30000, // Timeout for acquiring a connection
                }
            };

            this.logger.log(`Initializing new connection for ${dbType} database '${dbName}' with pool settings: max=10, min=1, idleTimeout=30s`);
            
            const newDataSource = new DataSource(newDataSourceOptions);
            await newDataSource.initialize();
            
            this.logger.log(`Connection established successfully to ${dbType} database '${dbName}'`);
            
            // Store with timestamps
            this.dataSourceCache[key] = {
                dataSource: newDataSource,
                createdAt: now,
                lastUsed: now,
                connectionsOpened: 1,
                queriesExecuted: 0
            };
            
            // Log current cache state
            this.logger.log(`Connection cache updated. Current size: ${Object.keys(this.dataSourceCache).length}`);
            
            return newDataSource;
        } else {
            // Store base data source in cache
            this.logger.log(`Caching base ${dbType} connection`);
            
            this.dataSourceCache[key] = {
                dataSource: baseDataSource,
                createdAt: now,
                lastUsed: now,
                connectionsOpened: 1,
                queriesExecuted: 0
            };
            
            return baseDataSource;
        }
    }

    async createDatabase(dbType: string, dbName: string): Promise<string> {
        let query: string;
        let checkQuery: string;
        this.logger.log(`Creating database: DB Type=${dbType}, DB Name=${dbName}`);
        let baseDataSource: DataSource;
    
        switch (dbType.toLowerCase()) {
            case "postgres":
                baseDataSource = this.postgresDataSource;
                checkQuery = `SELECT 1 FROM pg_database WHERE datname = '${dbName}'`;
                query = `CREATE DATABASE "${dbName}"`;
                break;
            // case "mysql":
            //     baseDataSource = this.mysqlDataSource;
            //     checkQuery = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${dbName}'`;
            //     query = `CREATE DATABASE IF NOT EXISTS \`${dbName}\``;
            //     break;
            // case "oracle":
            //     baseDataSource = this.oracleDataSource;
            //     checkQuery = `SELECT USERNAME FROM ALL_USERS WHERE USERNAME = UPPER('${dbName}')`;
            //     query = `CREATE USER ${dbName} IDENTIFIED BY password`;
            //     break;
            default:
                throw new Error(`Unsupported database type: ${dbType}`);
        }
    
        const queryRunner = baseDataSource.createQueryRunner();
    
        try {
            this.logger.log(`Connecting to ${dbType} for database creation`);
            await queryRunner.connect();
            
            // Check if database exists
            this.logger.log(`Checking if database '${dbName}' exists`);
            const exists = await queryRunner.query(checkQuery);
            
            if (exists && exists.length > 0) {
                this.logger.log(`Database '${dbName}' already exists`);
                return `Database "${dbName}" already exists`;
            }
    
            // Create database if it doesn't exist
            this.logger.log(`Creating database '${dbName}'`);
            await queryRunner.query(query);
            this.logger.log(`Database '${dbName}' created successfully`);
            return `Database ${dbName} created successfully!`;
        } catch (error) {
            this.logger.error(`Error creating database: ${error.message}`);
            // Check if error is about database already existing
            if (error.message && error.message.includes('already exists')) {
                return `Database ${dbName} already exists`;
            }
            throw new Error(`Error creating database: ${error.message}`);
        } finally {
            this.logger.log(`Releasing connection for database creation`);
            await queryRunner.release();
        }
    }

    async createOrUpdateTable(dbType: string, tableName: string, columns: any[], dbName?: string) {
        this.logger.log(`Creating/updating table '${tableName}' in ${dbType} database${dbName ? ` '${dbName}'` : ''}`);
        
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            this.logger.log(`Connecting to ${dbType} for table operation on '${tableName}'`);
            await queryRunner.connect();
        
            if (dbType.toLowerCase() === 'oracle') {
                tableName = tableName.toUpperCase();
                this.logger.log(`Using uppercase table name for Oracle: '${tableName}'`);
            }
        
            const tableExists = await queryRunner.hasTable(tableName);
            if (tableExists) {
                this.logger.log(`Table '${tableName}' exists, checking for schema updates`);
                
                const table = await queryRunner.getTable(tableName);
                if (!table) {
                    this.logger.error(`Table '${tableName}' existence check returned true but table object is null`);
                    return {
                        statusCode: 400,
                        message: `Table ${tableName} does not exist`,
                    };
                }
        
                // Check for columns to add or update
                for (const col of columns) {
                    const existingColumn = table.columns.find(c => c.name === col.name);
                    if (!existingColumn) {
                        // Add new column if it doesn't exist
                        this.logger.log(`Adding new column '${col.name}' (${col.type}) to table '${tableName}'`);
                        await queryRunner.addColumn(
                            tableName,
                            new TableColumn({
                                name: col.name,
                                type: col.type,
                                isNullable: col.isNullable || true,
                                isUnique: col.isUnique || false,
                            }),
                        );
                    } else if (existingColumn.type.toLowerCase() !== col.type.toLowerCase()) {
                        this.logger.log(`Column type change requested: '${col.name}' from ${existingColumn.type} to ${col.type}`);
                        
                        // Check if data exists in the column
                        this.logger.log(`Checking if data exists in column '${col.name}' before type change`);
                        const hasData = await queryRunner.manager.query(`SELECT 1 FROM ${tableName} WHERE "${col.name}" IS NOT NULL`);
                        if (hasData.length > 0) {
                            this.logger.warn(`Cannot change column type: data exists in column '${col.name}'`);
                            return {
                                statusCode: 400,
                                message: `Cannot change column type from ${existingColumn.type} to ${col.type} as data exists in column ${col.name}`,
                            };
                        } else {
                            // Change column type if no data exists
                            this.logger.log(`Changing column '${col.name}' type from ${existingColumn.type} to ${col.type}`);
                            await queryRunner.changeColumn(
                                tableName,
                                existingColumn.name,
                                new TableColumn({
                                    name: col.name,
                                    type: col.type,
                                    isNullable: col.isNullable || true,
                                    isUnique: col.isUnique || false,
                                }),
                            );
                        }
                    }
                }
        
                // Check for columns to remove
                for (const existingCol of table.columns) {
                    // Skip the primary key column
                    if (existingCol.isPrimary) continue;
        
                    const columnStillExists = columns.some(col => col.name === existingCol.name);
        
                    if (!columnStillExists) {
                        // Drop column if it's not in the new schema
                        this.logger.log(`Dropping column '${existingCol.name}' from table '${tableName}'`);
                        await queryRunner.dropColumn(tableName, existingCol.name);
                    }
                }
                
                this.logger.log(`Table '${tableName}' updated successfully`);
            } else {
                // Table does not exist, create it
                this.logger.log(`Table '${tableName}' does not exist, creating it`);
                await queryRunner.createTable(
                    new Table({
                        name: tableName,
                        columns: columns.map(col => ({
                            name: col.name,
                            type: col.type,
                            isPrimary: col.isPrimary || false, // Set primary key if specified
                            isNullable: col.isNullable || true,
                            isUnique: col.isUnique || false,
                        })),
                    }),
                );
                
                this.logger.log(`Table '${tableName}' created successfully with ${columns.length} columns`);
            }
        
            return {
                statusCode: 200,
                message: `Table ${tableName} created/updated successfully`,
            };
        } catch (error) {
            this.logger.error(`Failed to create/update table '${tableName}': ${error.message}`);
            throw new Error(`Failed to create/update table ${tableName}: ${error.message}`);
        } finally {
            this.logger.log(`Releasing connection for table operation on '${tableName}'`);
            await queryRunner.release();
        }
    }

    async insertData(dbType: string, tableName: string, data: Record<string, any>, dbName?: string) {
        this.logger.log(`Inserting data into table '${tableName}' in ${dbType} database${dbName ? ` '${dbName}'` : ''}`);
        
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            this.logger.log(`Connecting to ${dbType} for data insertion on '${tableName}'`);
            await queryRunner.connect();
            
            this.logger.log(`Executing insert query on table '${tableName}'`);
            const result = await queryRunner.manager
                .createQueryBuilder()
                .insert()
                .into(tableName)
                .values(data)
                .execute();
            
            this.logger.log(`Data insertion complete. Affected rows: ${result.raw.affectedRows || result.raw.rowCount || 'unknown'}`);
                
            return { 
                message: `Data inserted into ${tableName} on ${dbType}`,
                affectedRows: result.raw.affectedRows || result.raw.rowCount || 1
            };
        } catch (error) {
            this.logger.error(`Failed to insert data into '${tableName}': ${error.message}`);
            throw new Error(`Failed to insert data: ${error.message}`);
        } finally {
            this.logger.log(`Releasing connection for data insertion on '${tableName}'`);
            await queryRunner.release();
        }
    }

    async getData(dbType: string, tableName: string, filters?: Record<string, any>, dbName?: string) {
        this.logger.log(`Fetching data from table '${tableName}' in ${dbType} database${dbName ? ` '${dbName}'` : ''}`);
        if (filters) {
            this.logger.log(`Filters applied: ${JSON.stringify(filters)}`);
        }
        
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            this.logger.log(`Connecting to ${dbType} for data retrieval from '${tableName}'`);
            await queryRunner.connect();
            
            const queryBuilder = queryRunner.manager
                .createQueryBuilder()
                .select('*')
                .from(tableName, tableName);
    
            if (filters) {
                Object.keys(filters).forEach((key) => {
                    queryBuilder.andWhere(`${key} = :${key}`, { [key]: filters[key] });
                });
            }
    
            this.logger.log(`Executing select query on table '${tableName}'`);
            const result = await queryBuilder.getRawMany();
            
            this.logger.log(`Data retrieval complete. Fetched ${result.length} records`);
            
            return result;
        } catch (error) {
            this.logger.error(`Failed to fetch data from '${tableName}': ${error.message}`);
            throw new Error(`Failed to fetch data: ${error.message}`);
        } finally {
            this.logger.log(`Releasing connection for data retrieval from '${tableName}'`);
            await queryRunner.release();
        }
    }

    async findAndUpdate(dbType: string, tableName: string, field: string, value: any, newData: Record<string, any>, dbName?: string) {
        this.logger.log(`Updating data in table '${tableName}' in ${dbType} database${dbName ? ` '${dbName}'` : ''}`);
        this.logger.log(`Update condition: ${field} = ${value}`);
        this.logger.log(`New data: ${JSON.stringify(newData)}`);
        
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            await queryRunner.connect();
            this.logger.log(`Connected to ${dbType} for update operation on '${tableName}'`);
            
            const result = await queryRunner.manager
                .createQueryBuilder()
                .update(tableName)
                .set(newData)
                .where(`"${field}" = :value`, { value: value })
                .execute();
            
            this.logger.log(`Update operation complete. Affected rows: ${result.affected}`);
            
            return { 
                success: true,
                message: `Data updated in ${tableName} on ${dbType}`,
                condition: `${field} = ${value}`,
                affectedRows: result.affected
            };
        } catch (error) {
            this.logger.error(`Failed to update data in '${tableName}': ${error.message}`);
            throw new Error(`Failed to update data: ${error.message}`);
        } finally {
            this.logger.log(`Releasing connection for update operation on '${tableName}'`);
            await queryRunner.release();
        }
    }

    async deleteData(dbType: string, tableName: string, filter: Record<string, any>, dbName?: string) {
        this.logger.log(`Deleting data from table '${tableName}' in ${dbType} database${dbName ? ` '${dbName}'` : ''}`);
        this.logger.log(`Delete condition: ${JSON.stringify(filter)}`);
        
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            await queryRunner.connect();
            this.logger.log(`Connected to ${dbType} for delete operation on '${tableName}'`);
            
            const result = await queryRunner.manager
                .createQueryBuilder()
                .delete()
                .from(tableName)
                .where(filter)
                .execute();
            
            this.logger.log(`Delete operation complete. Affected rows: ${result.affected}`);
            
            return { 
                message: `Record deleted from ${tableName} on ${dbType}`,
                affectedRows: result.affected
            };
        } catch (error) {
            this.logger.error(`Failed to delete data from '${tableName}': ${error.message}`);
            throw new Error(`Failed to delete data: ${error.message}`);
        } finally {
            this.logger.log(`Releasing connection for delete operation on '${tableName}'`);
            await queryRunner.release();
        }
    }

    async manualCleanup() {
        this.logger.log('Manual cleanup initiated');
        const cleanupResult = await this.cleanupOldConnections();
        this.logger.log('Manual cleanup completed');
        return cleanupResult;
    }
    
    getConnectionStats() {
        this.logger.log('Fetching connection statistics');
        const stats = {
            totalConnections: Object.keys(this.dataSourceCache).length,
            connectionDetails: {} as Record<string, { createdAt: string, lastUsed: string, idleTime: string }>
        };
        
        const now = Date.now();
        for (const key in this.dataSourceCache) {
            const cache = this.dataSourceCache[key];
            const idleTimeMs = now - cache.lastUsed;
            
            stats.connectionDetails[key] = {
                createdAt: new Date(cache.createdAt).toISOString(),
                lastUsed: new Date(cache.lastUsed).toISOString(),
                idleTime: `${Math.floor(idleTimeMs / 60000)} minutes ${Math.floor((idleTimeMs % 60000) / 1000)} seconds`
            };
        }
        
        this.logger.log(`Connection statistics: ${JSON.stringify(stats)}`);
        return stats;
    }
}