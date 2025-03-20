import { Injectable, Inject } from '@nestjs/common';
import { DataSource, Table, TableColumn } from 'typeorm';
import { MongoDBService } from '../utils/mongodbService';

@Injectable()
export class DynamicNoSQLDbService {
    constructor(
        private mongoDBService: MongoDBService,
    ) { }

    
    // Helper method to get the correct data source
    private getDataSource(dbType: string) {
        console.log('dbType', dbType);
        switch (dbType.toLowerCase()) {
            case 'mongodb':
                return this.mongoDBService;
            default:
                throw new Error('Unsupported database type');
        }
    }

    async createOrUpdateTable(dbType: string,tableName: string, columns: { name: string; type: string }[],dbName?: string){
        const service = this.getDataSource(dbType);
        if(!service){
            throw new Error(`Database not responding`);
        }
        await service.createOrUpdateTable(tableName,);
    }

    // ✅ Insert Data into Any Table
    async insertData(dbType: string, tableName: string, data: Record<string, any>) {
        const service = this.getDataSource(dbType);
        if(!service){
            throw new Error(`Database not responding`);
        }
        await service.insertData(tableName, data);
    }

    // ✅ Get Data from Any Table
    async getData(dbType: string, tableName: string, filters?: Record<string, any>) {
        const service = this.getDataSource(dbType);
        if(!service){
            throw new Error(`Database not responding`);
        }
        return await service.getData(tableName);
    }

    // ✅ Update Data in Any Table
    async findAndUpdate( dbType: string,  tableName: string,  field: string, value: any ,  newData: Record<string, any>) {
        const service = this.getDataSource(dbType);
        if(!service){
            throw new Error(`Database not responding`);
        }
        await service.findAndUpdate(tableName, field, value, newData);
    }

    // ✅ Delete Data from Any Table
    async deleteData(dbType: string, tableName: string, id: number) {
        const service = this.getDataSource(dbType);
        if(!service){
            throw new Error(`Database not responding`);
        }
        // await service.deleteData(tableName, id);
    }

    async createDatabase(dbType: string, dbName: string){

    }
}