import { Injectable, Inject, Optional } from '@nestjs/common'
import { SQLDbService } from './sqlDatabaseService';
import { DynamicNoSQLDbService } from './noSQLdatabaseService';

export type DatabaseType = 'mongodb' | 'postgres' | 'mysql' | 'oracle';

@Injectable()
export class DatabaseResolver {
    private readonly sqlTypes = ['postgres', 'mysql', 'oracle'] as const;
    private readonly noSqlTypes = ['mongodb'] as const;

    constructor(
        private readonly sqlDbService: SQLDbService,
        private readonly noSqlDbService: DynamicNoSQLDbService
    ) { }

    getDbService(type: DatabaseType) {
        if (this.noSqlTypes.includes(type as any)) {
            return this.noSqlDbService;
        }
        if (this.sqlTypes.includes(type as any)) {
            return this.sqlDbService;
        }
        throw new Error(`Unsupported database type: ${type}`);
    }

    async createDatabase(dbType: DatabaseType, dbName: string) {
        const service = this.getDbService(dbType);
        if (!service) {
            throw new Error(`Database not responding`);
        }
        return service.createDatabase(dbType, dbName);
    }
    
    // Unified methods for common operations across databases
    async getItem(options: { dbType: DatabaseType; collection: string; id: string; dbName?: string; }) {
        const service: any = this.getDbService(options.dbType);
        if (!service) {
            throw new Error(`Database not responding`);
        }
        return service.getById(options.collection, options.id, options.dbName);
    }

    async createOrUpdateTable(dbType: any, tableName: string, columns: { name: string; type: string }[], dbName?: string) {
        const service = this.getDbService(dbType);
        if (!service) {
            throw new Error(`Database not responding`);
        }
        console.log('DatabaseResolver dbType', dbType);
        // Fix: Pass dbType as first argument
        return service.createOrUpdateTable(dbType, tableName, columns, dbName);
    }

    async insertData(dbType: any, tableName: string, data: any) {

        const service = this.getDbService(dbType);
        if (!service) {
            throw new Error(`Database not responding`);
        }
        return await service.insertData(dbType, tableName, data);
    }

    async getData(dbType: any, tableName: string, dbName?: string) {
        const service = this.getDbService(dbType);
        if (!service) {
            throw new Error(`Database not responding`);
        }
        return await service.getData(dbType, tableName, {}, dbName);
    }

    async findAndUpdate(dbType: any, tableName: string, fieldName: string, fieldValue: string, data: any) {
        const service = this.getDbService(dbType);
        if (!service) {
            throw new Error(`Database not responding`);
        }
        return await service.findAndUpdate(dbType, tableName, fieldName, fieldValue, data);
    }
}