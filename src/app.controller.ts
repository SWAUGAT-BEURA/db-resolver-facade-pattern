import { Controller, Get, Post, Put, Delete, Body, Param, Query, HttpException, HttpStatus, Logger } from '@nestjs/common';
import { AppService } from './app.service';
import { SQLDbService } from './core/database/sqlDatabaseService'; // Import the SQLDbService

// Define DTOs for better type safety
class CreateTableDto {
  columns: Array<{
    name: string;
    type: string;
    isPrimary?: boolean;
    isNullable?: boolean;
    isUnique?: boolean;
  }>;
}

class InsertDataDto {
  data: Record<string, any>;
}

class UpdateDataDto {
  field: string;
  value: any;
  newData: Record<string, any>;
}

class DeleteDataDto {
  filter: Record<string, any>;
}

@Controller('api')
export class AppController {
  private readonly logger = new Logger(AppController.name);
  
  constructor(
    private readonly appService: AppService,
    private readonly sqlDbService: SQLDbService, // Inject SQLDbService
  ) {}

  @Get()
  getHello(): string {
    return this.appService.getHello();
  }

  // Endpoint to get connection stats
  @Get('connection-stats')
  async getConnectionStats() {
    try {
      const stats = this.sqlDbService.getConnectionStats();
      return {
        success: true,
        message: 'Connection stats retrieved successfully',
        stats
      };
    } catch (error) {
      this.logger.error(`Failed to get connection stats: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to get connection stats: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Endpoint to manually trigger connection cleanup
  @Post('cleanup-connections')
  async cleanupConnections() {
    try {
      await this.sqlDbService.manualCleanup();
      return {
        success: true,
        message: 'Connection cleanup triggered successfully'
      };
    } catch (error) {
      this.logger.error(`Failed to cleanup connections: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to cleanup connections: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Create or update table for a specific tenant
  @Post(':tenant/tables/:tableName')
  async createOrUpdateTable(
    @Param('tenant') tenant: string,
    @Param('tableName') tableName: string,
    @Body() createTableDto: CreateTableDto
  ) {
    try {
      this.logger.log(`Creating/updating table '${tableName}' for tenant '${tenant}'`);
      
      // Determine which tenant database to use
      const dbInfo = this.getTenantDbInfo(tenant);
      
      const result = await this.sqlDbService.createOrUpdateTable(
        dbInfo.dbType,
        tableName,
        createTableDto.columns,
        dbInfo.dbName
      );
      
      return {
        success: true,
        message: `Table ${tableName} created/updated for tenant ${tenant}`,
        result
      };
    } catch (error) {
      this.logger.error(`Failed to create/update table: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to create/update table: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Insert data into a table for a specific tenant
  @Post(':tenant/tables/:tableName/data')
  async insertData(
    @Param('tenant') tenant: string,
    @Param('tableName') tableName: string,
    @Body() insertDataDto: InsertDataDto
  ) {
    try {
      this.logger.log(`Inserting data into '${tableName}' for tenant '${tenant}'`);
      
      // Determine which tenant database to use
      const dbInfo = this.getTenantDbInfo(tenant);
      
      // Check if the insertDataDto.data includes an 'id' field
      if (insertDataDto.data.id !== undefined) {
        this.logger.warn(`'id' field is included in the insert data. Ensure this is intentional.`);
      }

      const result = await this.sqlDbService.insertData(
        dbInfo.dbType,
        tableName,
        insertDataDto.data,
        dbInfo.dbName
      );
      
      return {
        success: true,
        message: `Data inserted into ${tableName} for tenant ${tenant}`,
        result
      };
    } catch (error) {
      this.logger.error(`Failed to insert data: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to insert data: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Get data from a table for a specific tenant
  @Get(':tenant/tables/:tableName/data')
  async getData(
    @Param('tenant') tenant: string,
    @Param('tableName') tableName: string,
    @Query() filters: Record<string, any>
  ) {
    try {
      this.logger.log(`Fetching data from '${tableName}' for tenant '${tenant}'`);
      
      // Determine which tenant database to use
      const dbInfo = this.getTenantDbInfo(tenant);
      
      // Only apply filters if they exist
      const queryFilters = Object.keys(filters).length > 0 ? filters : undefined;
      
      const result = await this.sqlDbService.getData(
        dbInfo.dbType,
        tableName,
        queryFilters,
        dbInfo.dbName
      );
      
      return {
        success: true,
        message: `Data retrieved from ${tableName} for tenant ${tenant}`,
        data: result
      };
    } catch (error) {
      this.logger.error(`Failed to get data: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to get data: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Update data in a table for a specific tenant
  @Put(':tenant/tables/:tableName/data')
  async updateData(
    @Param('tenant') tenant: string,
    @Param('tableName') tableName: string,
    @Body() updateDataDto: UpdateDataDto
  ) {
    try {
      this.logger.log(`Updating data in '${tableName}' for tenant '${tenant}'`);
      
      // Determine which tenant database to use
      const dbInfo = this.getTenantDbInfo(tenant);
      
      const result = await this.sqlDbService.findAndUpdate(
        dbInfo.dbType,
        tableName,
        updateDataDto.field,
        updateDataDto.value,
        updateDataDto.newData,
        dbInfo.dbName
      );
      
      return {
        success: true,
        message: `Data updated in ${tableName} for tenant ${tenant}`,
        result
      };
    } catch (error) {
      this.logger.error(`Failed to update data: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to update data: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Delete data from a table for a specific tenant
  // Delete data from a table for a specific tenant
  @Delete(':tenant/tables/:tableName/data')
  async deleteData(
    @Param('tenant') tenant: string,
    @Param('tableName') tableName: string,
    @Body() deleteDataDto: DeleteDataDto
  ) {
    try {
      this.logger.log(`Deleting data from '${tableName}' for tenant '${tenant}' with filter: ${JSON.stringify(deleteDataDto.filter)}`);
      
      // Determine which tenant database to use
      const dbInfo = this.getTenantDbInfo(tenant);
      
      const result = await this.sqlDbService.deleteData(
        dbInfo.dbType,
        tableName,
        deleteDataDto.filter,
        dbInfo.dbName
      );
      
      return {
        success: true,
        message: `Data deleted from ${tableName} for tenant ${tenant}`,
        result
      };
    } catch (error) {
      this.logger.error(`Failed to delete data: ${error.message}`);
      throw new HttpException(
        { success: false, message: `Failed to delete data: ${error.message}` },
        HttpStatus.INTERNAL_SERVER_ERROR
      );
    }
  }

  // Helper method to determine the database type and name based on tenant
  private getTenantDbInfo(tenant: string): { dbType: string, dbName: string } {
    switch (tenant.toLowerCase()) {
      case 'tenant1':
        return {
          dbType: 'postgres',
          dbName: process.env.POSTGRES_DB || 'tenant1'
        };
      case 'tenant2':
        return {
          dbType: 'postgres',
          dbName: process.env.TENANT_3_TEST || 'TENANT_3_TEST'
        };
      default:
        throw new HttpException(
          { success: false, message: `Unknown tenant: ${tenant}` },
          HttpStatus.BAD_REQUEST
        );
    }
  }
}