import { Module, Global } from '@nestjs/common';
import { POSTGRES_CONNECTION } from 'src/config/config';
import { DataSource } from 'typeorm';


@Global()
@Module({
    providers: [
        {
            provide: 'POSTGRES_CONNECTION',
            useFactory: async () => {
                const dataSource = new DataSource({
                    name: 'postgres',
                    type: 'postgres',
                    host: POSTGRES_CONNECTION.POSTGRES_HOST,
                    port: 5432,
                    username: POSTGRES_CONNECTION.POSTGRES_USER,
                    password: POSTGRES_CONNECTION.POSTGRES_PASSWORD,
                    database: POSTGRES_CONNECTION.POSTGRES_DB,
                });
                return dataSource.initialize();
            },
        },
        // {
        //     provide: 'MYSQL_CONNECTION',
        //     useFactory: async () => {
        //         const dataSource = new DataSource({
        //             type: 'mysql',
        //             host: process.env.MYSQL_HOST,
        //             port: 3306,
        //             username: process.env.MYSQL_USER,
        //             password: process.env.MYSQL_PASSWORD,
        //             database: process.env.MYSQL_DB, // Corrected from MYSQL_PASSWORD to MYSQL_DB
        //         });
        //         return dataSource.initialize();
        //     }
        // },
        // {
        //     provide: 'ORACLE_CONNECTION',
        //     useFactory: async () => {
        //         const dataSource = new DataSource({
        //             type: 'oracle',
        //             host: process.env.ORACLE_HOST,
        //             port: 1521,
        //             username: process.env.ORACLE_USER,
        //             password: process.env.ORACLE_PASSWORD,
        //             database: process.env.ORACLE_DB,
        //             connectString: `${process.env.ORACLE_HOST}:${process.env.ORACLE_PORT}/DB1`,
        //         });
        //         return dataSource.initialize();
        //     },
        // }
    ],
    // exports: ['POSTGRES_CONNECTION', 'MYSQL_CONNECTION', 'ORACLE_CONNECTION'], // Corrected typo here
    exports: ['POSTGRES_CONNECTION']
})
export class DatabaseModule {}