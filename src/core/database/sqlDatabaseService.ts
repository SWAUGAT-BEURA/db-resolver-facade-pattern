import { Inject, Injectable, OnModuleInit, OnModuleDestroy } from "@nestjs/common";
import { DataSource, Table, TableColumn } from "typeorm";

@Injectable()
export class SQLDbService implements OnModuleInit, OnModuleDestroy {
    constructor(
        @Inject('POSTGRES_CONNECTION') private postgresDataSource: DataSource,
        @Inject('MYSQL_CONNECTION') private mysqlDataSource: DataSource,
        @Inject('ORACLE_CONNECTION') private oracleDataSource: DataSource,
    ) {}

    // Cache to store data sources with timestamps and connection info
    private dataSourceCache: Record<string, {
        dataSource: DataSource,
        createdAt: number,
        lastUsed: number
    }> = {};

    // Configuration for cache management
    private readonly MAX_CACHE_SIZE = 20; // Maximum number of cached connections
    private readonly CACHE_TTL = 10 * 60 * 1000; // 10 minutes before connection cleanup
    private readonly CLEANUP_INTERVAL = 60 * 60 * 1000; // Run cleanup every 1 hr
    private cleanupInterval: NodeJS.Timeout | null = null;

    async onModuleInit() {
        // Start the cleanup interval when the service initializes
        this.cleanupInterval = setInterval(() => this.cleanupOldConnections(), this.CLEANUP_INTERVAL);
    }

    async onModuleDestroy() {
        // Clear the cleanup interval when the module is destroyed
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
        
        // Close all cached connections
        for (const key in this.dataSourceCache) {
            try {
                const cache = this.dataSourceCache[key];
                await cache.dataSource.destroy();
            } catch (error) {
                console.error(`Error destroying connection ${key}:`, error);
            }
        }
    }

    private async cleanupOldConnections() {
        console.log('Running connection cache cleanup...');
        const now = Date.now();
        const keysToRemove: string[] = [];
        
        // Find connections that haven't been used for a while
        for (const key in this.dataSourceCache) {
            const cache = this.dataSourceCache[key];
            
            // Skip base connections (the ones without a specific database name)
            if (key.endsWith('_default')) {
                continue;
            }
            
            // Check if the connection has been idle for too long
            if (now - cache.lastUsed > this.CACHE_TTL) {
                keysToRemove.push(key);
            }
        }
        
        // Remove and destroy old connections
        for (const key of keysToRemove) {
            console.log(`Closing idle connection: ${key}`);
            try {
                await this.dataSourceCache[key].dataSource.destroy();
                delete this.dataSourceCache[key];
            } catch (error) {
                console.error(`Error destroying connection ${key}:`, error);
            }
        }
        
        console.log(`Cleanup completed. Removed ${keysToRemove.length} connections. Remaining: ${Object.keys(this.dataSourceCache).length}`);
    }

    private async enforceMaxCacheSize() {
        const keys = Object.keys(this.dataSourceCache);
        
        // Skip if we're under the limit
        if (keys.length <= this.MAX_CACHE_SIZE) {
            return;
        }
        
        // Sort by last used time (oldest first)
        const sortedKeys = keys
            .filter(key => !key.endsWith('_default')) // Skip base connections
            .sort((a, b) => this.dataSourceCache[a].lastUsed - this.dataSourceCache[b].lastUsed);
        
        // Remove connections until we're under the limit
        const keysToRemove = sortedKeys.slice(0, keys.length - this.MAX_CACHE_SIZE);
        
        for (const key of keysToRemove) {
            console.log(`Removing connection due to cache size limit: ${key}`);
            try {
                await this.dataSourceCache[key].dataSource.destroy();
                delete this.dataSourceCache[key];
            } catch (error) {
                console.error(`Error destroying connection ${key}:`, error);
            }
        }
    }

    private async getDataSource(dbType: string, dbName?: string): Promise<DataSource> {
        const key = `${dbType.toLowerCase()}_${dbName || 'default'}`;
        const now = Date.now();
        
        // Return from cache if available
        if (this.dataSourceCache[key]) {
            // Update last used timestamp
            this.dataSourceCache[key].lastUsed = now;
            return this.dataSourceCache[key].dataSource;
        }

        let baseDataSource: DataSource;
        switch (dbType.toLowerCase()) {
            case 'postgres':
                baseDataSource = this.postgresDataSource;
                break;
            case 'mysql':
                baseDataSource = this.mysqlDataSource;
                break;
            case 'oracle':
                baseDataSource = this.oracleDataSource;
                break;
            default:
                throw new Error('Unsupported database type');
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
                }
            };

            const newDataSource = new DataSource(newDataSourceOptions);
            await newDataSource.initialize();
            
            // Store with timestamps
            this.dataSourceCache[key] = {
                dataSource: newDataSource,
                createdAt: now,
                lastUsed: now
            };
            
            return newDataSource;
        } else {
            // Store base data source in cache
            this.dataSourceCache[key] = {
                dataSource: baseDataSource,
                createdAt: now,
                lastUsed: now
            };
            
            return baseDataSource;
        }
    }

    async createDatabase(dbType: string, dbName: string): Promise<string> {
        let query: string;
        let checkQuery: string;
        console.log('DB Type:', dbType, 'DB Name:', dbName);
        let baseDataSource: DataSource;
    
        switch (dbType.toLowerCase()) {
            case "postgres":
                baseDataSource = this.postgresDataSource;
                checkQuery = `SELECT 1 FROM pg_database WHERE datname = '${dbName}'`;
                query = `CREATE DATABASE "${dbName}"`;
                break;
            case "mysql":
                baseDataSource = this.mysqlDataSource;
                checkQuery = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${dbName}'`;
                query = `CREATE DATABASE IF NOT EXISTS \`${dbName}\``;
                break;
            case "oracle":
                baseDataSource = this.oracleDataSource;
                checkQuery = `SELECT USERNAME FROM ALL_USERS WHERE USERNAME = UPPER('${dbName}')`;
                query = `CREATE USER ${dbName} IDENTIFIED BY password`;
                break;
            default:
                throw new Error(`Unsupported database type: ${dbType}`);
        }
    
        const queryRunner = baseDataSource.createQueryRunner();
    
        try {
            await queryRunner.connect();
            
            // Check if database exists
            const exists = await queryRunner.query(checkQuery);
            
            if (exists && exists.length > 0) {
                return `Database "${dbName}" already exists`;
            }
    
            // Create database if it doesn't exist
            await queryRunner.query(query);
            return `Database ${dbName} created successfully!`;
        } catch (error) {
            console.log('Error:', error);
            // Check if error is about database already existing
            if (error.message && error.message.includes('already exists')) {
                return `Database ${dbName} already exists`;
            }
            throw new Error(`Error creating database: ${error.message}`);
        } finally {
            await queryRunner.release();
        }
    }

    async createOrUpdateTable(dbType: string, tableName: string, columns: any[], dbName?: string) {
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            await queryRunner.connect();
        
            if (dbType.toLowerCase() === 'oracle') {
                tableName = tableName.toUpperCase();
            }
        
            const tableExists = await queryRunner.hasTable(tableName);
            if (tableExists) {
                const table = await queryRunner.getTable(tableName);
                if (!table) {
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
                        // Check if data exists in the column
                        const hasData = await queryRunner.manager.query(`SELECT 1 FROM ${tableName} WHERE "${col.name}" IS NOT NULL`);
                        if (hasData.length > 0) {
                            return {
                                statusCode: 400,
                                message: `Cannot change column type from ${existingColumn.type} to ${col.type} as data exists in column ${col.name}`,
                            };
                        } else {
                            // Change column type if no data exists
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
                        await queryRunner.dropColumn(tableName, existingCol.name);
                    }
                }
            } else {
                // Table does not exist, create it
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
            }
        
            return {
                statusCode: 200,
                message: `Table ${tableName} created/updated successfully`,
            };
        } catch (error) {
            throw new Error(`Failed to create/update table ${tableName}: ${error.message}`);
        } finally {
            await queryRunner.release();
        }
    }

    async insertData(dbType: string, tableName: string, data: Record<string, any>, dbName?: string) {
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            await queryRunner.connect();
            
            await queryRunner.manager
                .createQueryBuilder()
                .insert()
                .into(tableName)
                .values(data)
                .execute();
                
            return { message: `Data inserted into ${tableName} on ${dbType}` };
        } catch (error) {
            throw new Error(`Failed to insert data: ${error.message}`);
        } finally {
            await queryRunner.release();
        }
    }

    async getData(dbType: string, tableName: string, filters?: Record<string, any>, dbName?: string) {
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
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
    
            const result = await queryBuilder.getRawMany();
            return result;
        } catch (error) {
            throw new Error(`Failed to fetch data: ${error.message}`);
        } finally {
            await queryRunner.release();
        }
    }

    async findAndUpdate(dbType: string, tableName: string, field: string, value: any, newData: Record<string, any>, dbName?: string) {
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            await queryRunner.connect();
            
            await queryRunner.manager
                .createQueryBuilder()
                .update(tableName)
                .set(newData)
                .where(`"${field}" = :value`, { value: value })
                .execute();
                
            return { 
                success: true,
                message: `Data updated in ${tableName} on ${dbType}`,
                condition: `${field} = ${value}`
            };
        } catch (error) {
            throw new Error(`Failed to update data: ${error.message}`);
        } finally {
            await queryRunner.release();
        }
    }

    async deleteData(dbType: string, tableName: string, id: number, dbName?: string) {
        const dataSource = await this.getDataSource(dbType, dbName);
        const queryRunner = dataSource.createQueryRunner();
        
        try {
            await queryRunner.connect();
            
            await queryRunner.manager
                .createQueryBuilder()
                .delete()
                .from(tableName)
                .where('id = :id', { id })
                .execute();
                
            return { message: `Record deleted from ${tableName} on ${dbType}` };
        } catch (error) {
            throw new Error(`Failed to delete data: ${error.message}`);
        } finally {
            await queryRunner.release();
        }
    }

    // Method to allow manual cleanup - useful for testing or on-demand cleanup
    async manualCleanup() {
        return this.cleanupOldConnections();
    }
    
    // Method to get current connection cache stats - useful for monitoring
    getConnectionStats() {
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
        
        return stats;
    }
}