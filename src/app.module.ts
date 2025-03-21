import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { SQLDbService } from './core/database/sqlDatabaseService';
import { DatabaseModule } from './core/database/databaseModule';

@Module({
  imports: [DatabaseModule],
  controllers: [AppController],
  providers: [AppService,SQLDbService],
})
export class AppModule {}
