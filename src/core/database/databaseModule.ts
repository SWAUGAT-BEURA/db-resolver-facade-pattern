import {Module, Global} from '@nestjs/common'
import { DataSource } from 'typeorm'

@Global()
@Module({
    providers:[
        {
            provide:'POSTGRES_CONNECTION',
            useFactory: async () => {
                const dataSource = new DataSource({
                    name:'postgres',
                    type: 'postgres',
                    host: process.env.POSTGRES_HOST,
                    port: 5432,
                    username: process.env.POSTGRES_USER,
                    password: process.env.POSTGRES_PASSWORD,
                    database: process.env.POSTGRES_DB,
                });
                return dataSource.initialize();
            },
        },
        {
            provide:'MYSQL_CONNECTION',
            useFactory: async () => {
                const dataSource=new DataSource({
                    type: 'mysql',
                    host: process.env.MYSQL_HOST,
                    port: 3306,
                    username: process.env.MYSQL_USER,
                    password: process.env.MYSQL_PASSWORD,
                    database: process.env.MYSQL_PASSWORD
                });
                return dataSource.initialize();
            }
        },
        {
            provide: 'ORACLE_CONNECTION',
            useFactory: async () => {
                const dataSource =new DataSource({
                    type: 'oracle',
                    host: process.env.ORACLE_HOST,
                    port: 1521,
                    username: process.env.ORACLE_USER,
                    password: process.env.ORACLE_PASSWORD,
                    database: process.env.ORACLE_DB,
                    connectString: `${process.env.ORACLE_HOST}:${process.env.ORACLE_PORT}/DB1`,
                });
                return dataSource.initialize();
            },
        }
        
    ],
    exports:['POSTGRES_CONNECTION','ORACLE_CONNECTION','MYSQL_CONNECTON'],
})
export class DatabaseModule {}