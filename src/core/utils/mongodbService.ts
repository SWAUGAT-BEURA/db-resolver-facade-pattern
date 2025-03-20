import { MongoClient, Db, ObjectId, FindOneAndUpdateOptions, ClientSession,ReturnDocument } from 'mongodb';
import { Injectable } from '@nestjs/common';

import { MONGO_DB_NAME, MONGO_URI, ERROR_MESSAGES, JWT_KEY, TABLE_NAMES, NETWORK, TOKEN_ENCRYPTION_KEY} from '../../config/config';

import * as crypto from 'crypto';

@Injectable()
export class MongoDBService {
    private db: Db;
    private client: MongoClient;
    // private esClient: Client;
    constructor() {

        // const { maxPoolSize, minPoolSize, maxIdleTimeMS, socketTimeoutMS } = getConnectionOptions(NETWORK);

        // this.client = new MongoClient(getConnectionURI(NETWORK), {
        //     maxPoolSize,
        //     minPoolSize,
        //     maxIdleTimeMS,
        //     socketTimeoutMS,
        // });

        this.client = new MongoClient(MONGO_URI);
        this.connect().then(() => {
            this.db = this.client.db(MONGO_DB_NAME);
        });
    }

    async connect(): Promise<void> {
        try {
            await this.client.connect();
            this.db = this.client.db(MONGO_DB_NAME);
        } catch (error) {
            throw error;
        }
    }


    async createOrUpdateTable(collectionName: string, dbName?: any): Promise<any> {
        try {
            //create collection
            const db = dbName ? this.client.db(dbName) : this.db;
            await db.createCollection(collectionName);
            return { message: 'Collection created successfully' }
            
        } catch (error) {
            
        }
    }

    async insertData(collectionName: string, data: any, dbName?: any): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.insertOne(data);
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async getData(collectionName: string, dbName?: any): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.find().toArray();
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async findAndUpdate(collectionName: string, field: string, value: any, data: any, dbName?: any): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.updateOne({ [field]: value }, { $set: data });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async bulkUpdateOrCreate(collectionName: string, data: any[], dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.bulkWrite(data, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async startSession(): Promise<ClientSession> {
        try {
            await this.client.connect();
            return this.client.startSession();
        } catch (Error) {
            throw Error;
        }
    }

    async getItem(collectionName: string, id: string, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.findOne({ _id: new ObjectId(id) }, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async getAllItem(collectionName: string, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.find({}, { session }).toArray();
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async getAllTrails(collectionName: string, filter = {}, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.find(filter, { session }).toArray();
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async createItem(collectionName: string, data: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.insertOne(data, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async bulkCreateItems(
        collectionName: string,
        data: any[],
        dbName?: string,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.insertMany(data, { ordered: false, session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async createItemMany(collectionName: string, data: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.insertMany(data, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateItem(
        collectionName: string,
        keyName: string,
        keyValue: any,
        updateValues: any,
        dbName?: string,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const filter = keyName == "_id" ? { [keyName]: new ObjectId(keyValue) } : { [keyName]: keyValue };
            const update = { $set: updateValues };
            const collection = db.collection(collectionName);
            const res = await collection.updateOne(filter, update, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateItemQuery(
        collectionName: string,
        filter: any,
        updateValues: any,
        dbName?: string,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const update = { $set: updateValues };
            const collection = db.collection(collectionName);
            const res = await collection.updateOne(filter, update, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async addItemQuery(
        collectionName: string,
        filter: any,
        updateOperation: any,
        dbName?: string,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.updateOne(filter, updateOperation, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateItembyId(
        collectionName: string,
        _id: string | ObjectId,
        updateData: any,
        dbName?: string,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const id = typeof _id === 'string' ? new ObjectId(_id) : _id; // Handle both strings and ObjectId instances
            const res = await collection.updateOne(
                { _id: id },
                updateData,
                { session }
            );
            if (res.matchedCount === 0) {
                throw new Error('No document found with the provided _id');
            }
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async findOneAndUpdate(
        collectionName: string,
        filter: any,
        update: any,
        dbName?: string,
        options?: FindOneAndUpdateOptions & { arrayFilters?: any[] },
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const result = await collection.findOneAndUpdate(filter, update, { ...options, session });
            if (!result) {
                throw new Error('No document found with the provided filter');
            }
            return result;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async newfindOneAndUpdate(
        collectionName: string,
        filter: any,
        update: any,
        dbName?: string,
        options?: any,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const result = await collection.findOneAndUpdate(filter, update, {...options,session});
            if (!result) {
                throw new Error('No document found with the provided filter');
            }
            return result;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async deleteItem(collectionName: string, keyName: string, keyValue: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const filter = keyName == "_id" ? { [keyName]: new ObjectId(keyValue) } : { [keyName]: keyValue };
            const collection = db.collection(collectionName);
            const res = await collection.deleteOne(filter, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async deleteMany(collectionName: string, filter: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.deleteMany(filter, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async findByUniqueValue(collectionName: string, field: string, value: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const filter = { [field]: value };
            const res = await collection.findOne(filter, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async findAllDocument(collectionName: string, field: string, value: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const filter = { [field]: value };
            const res = await collection.find(filter, { session }).toArray();
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async verifyToken(token: string, secret: string): Promise<any> {
        return new Promise(async(resolve, reject) => {
            try {
                const decryptedData = await this.decryptToken(token);
                resolve(decryptedData);
            } catch (error) {
                reject(error);
            }
        });
    }

    private decryptToken(encryptedToken: string): any {
        try {
            const [ivBase64, ciphertextBase64] = encryptedToken.split(':');
            if (!ivBase64 || !ciphertextBase64) throw new Error('Invalid token format');

            // Convert URL-safe base64 to standard base64
            const iv = Buffer.from(ivBase64.replace(/-/g, '+').replace(/_/g, '/'), 'base64');
            const encryptedData = Buffer.from(ciphertextBase64.replace(/-/g, '+').replace(/_/g, '/'), 'base64');

            const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(TOKEN_ENCRYPTION_KEY, 'base64'), iv);
            const decrypted = Buffer.concat([decipher.update(encryptedData), decipher.final()]);

            try {
                const decryptedPayload = JSON.parse(decrypted.toString());
                if (decryptedPayload.expiry < new Date().getTime()) {
                    return null; // Token has expired
                }
                return decryptedPayload;
            } catch (error) {
                return null; // Failed to parse JSON
            }
        } catch (error) {
            console.error('Decryption failed:', error.message);
            return null;
        }
    }

    // async searchItemsInElasticsearch(collectionName: string, query: any): Promise<any[]> {
    //     try {
    //         const body = await this.esClient.search({
    //             index: collectionName,
    //             body: query
    //         });
    //         return body.hits.hits.map(hit => hit._source);
    //     } catch (err) {
    //         throw new Error(err.message);
    //     }
    // }
    // async findByQuery(collectionName: string, query: any, dbName?: string, session?: ClientSession): Promise<any[]> {
    //     try {
    //         const db = dbName ? this.client.db(dbName) : this.db;
    //         const collection = db.collection(collectionName);
    //         const queryResult = await collection.aggregate(query, { allowDiskUse: true, session }).toArray();
    //         return queryResult;
    //     } catch (err) {
    //         throw err; // Throw the error to be caught and handled in the calling code
    //     }
    // }
    async findByQuery(collectionName: string, query: any, dbName?: string, session?: ClientSession): Promise<any[]> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const queryResult = await collection.aggregate(query, {  session }).toArray();
            return queryResult;
        } catch (err) {
            throw err; // Throw the error to be caught and handled in the calling code
        }
    }
    async findByQueryPaginate(collectionName: string, query: any, page:number, limit:number, dbName?: string, session?: ClientSession): Promise<{ totalNumberOfPages: number; currentPage: number; data: any[]; }> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const queryResult = await collection.aggregate(query, { allowDiskUse: true, session }).toArray();
            const totalNumberOfPages = Math.ceil(queryResult.length / limit);
            const currentPage = page;
            const start = (page - 1) * limit;
            const end = page * limit;
            const paginatedData = queryResult.slice(start, end);
            return {
                totalNumberOfPages,
                currentPage,
                data: paginatedData
            };
        } catch (err) {
            throw err; // Throw the error to be caught and handled in the calling code
        }
    }
    async createIndexesForFields(collectionName: string, fields: string[], dbName?: string) {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            let field_name = {}
            const indexName = `compound_text_index`;
            for (const field of fields) {
                field_name[field] = 1
            }
            await collection.createIndex(field_name, { name: indexName });
        } catch (error) {
            throw error;
        }
    }

    async createDynamicIndexes(collectionName: string, fields: string[],indexType: string,dbName?: string,indexName?: string){
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            if (!Array.isArray(fields) || fields.length === 0) {
                throw new Error('Fields array must not be empty.');
            }
            const allowedIndexTypes = ['text', 'ascending', 'descending', 'hashed', '2dsphere'];
            if (!allowedIndexTypes.includes(indexType.toLowerCase())) {
                throw new Error('Invalid index type. Allowed types are text, ascending, descending, and hashed.');
            }
            let field_name = {}
            for(const field of fields){
                switch (indexType.toLowerCase()) {
                    case 'text':
                      field_name[field] = 'text';
                      break;
                    case 'ascending':
                      field_name[field] = 1;
                      break;
                    case 'descending':
                      field_name[field] = -1;
                      break;
                    case 'hashed':
                      field_name[field] = 'hashed';
                      break;
                    case '2dsphere':
                      field_name[field] = '2dsphere';
                      break;
                }
            }
            const indexOptions = indexName ? { name: indexName } : {};
            const response = await collection.createIndex(field_name, indexOptions);
            return response;
        } catch (error) {
            throw error;
        }
    }

    async createCollectionAndIndexes(collectionName: string, fields: string[], dbName?: string) {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            await db.createCollection(collectionName.toUpperCase()+'_DB');
            await this.createIndexesForFields(collectionName.toUpperCase()+'_DB', fields, dbName);
        } catch (error) {
            throw error;
        }
    }

    async paginationData(collectionName: string, tenantid: string, page: number = 1, limit: number = 10, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            let pipeline = [
                { $match: { tenant_id: tenantid } },
                { $addFields: { data: { $reverseArray: "$data" } } },
                { $unwind: "$data" },
                {
                    $group: {
                        _id: null,
                        data: { $push: "$data" },
                        total_Number_data: { $sum: 1 }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        data: { $slice: ["$data", (page - 1) * limit, { $toInt: limit }] },
                        total_Number_data: 1
                    }
                }
            ]
            const results = await collection.aggregate(pipeline, { session }).toArray();

            const totalPage = Math.ceil(results[0].total_Number_data / limit);

            return {
                total_Number_data: results[0].total_Number_data,
                page_no: page,
                total_Page: totalPage,
                page_size: limit,
                data: results[0].data
            };
        } catch (err) { console.log(err) }
    }

    async paginationFormData(collectionName: string, pipeline: any, tenantid: string, page: number = 1, limit: number = 10, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);

            const results = await collection.aggregate(pipeline, { session }).toArray();

            const totalPage = Math.ceil(results[0].total_Number_data / limit);

            return {
                totalRecords: results[0].total_Number_data,
                currentPage: page,
                total_Page: totalPage,
                limit: limit,
                data: results[0].data
            };
        } catch (err) { console.log(err) }
    }

    async searchFormData(collectionName: string, pipeline: any, tenantid: string, prefix: string = '', dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);

            const results = await collection.aggregate(pipeline, { session }).toArray();

            return {
                totalRecords: results[0]?.total_Number_data,
                data: results[0]?.data
            };
        } catch (err) { console.log(err) }
    }

    async aggregate(collectionName: string, pipeline: object[], schema?: string, session?: ClientSession) {
        try {
            const db = this.client.db(schema);
            const collection = db.collection(collectionName);
            const result = await collection.aggregate(pipeline, { session }).toArray();
            return result;
        } catch (error) {
            return null;
        }
    }

    async authenticate(token: string, paramValue: string, collectionName: string, dbName?: string): Promise<any> {
        if (!token) {
            return {
                statusCode: 400,
                message: ERROR_MESSAGES.TOKEN_UNAVAILABLE,
            };
        }
        const userDetails = await this.findOne(collectionName, { user_id: paramValue, isDeleted: {$ne:true} });
        if (!userDetails) {
            return {
                statusCode: 401,
                message: "User not found.",
            };
        }
        const decoded = await this.verifyToken(token, TOKEN_ENCRYPTION_KEY);
        if (decoded.user_id !== paramValue) {
            return {
                statusCode: 400,
                message: ERROR_MESSAGES.AUTH_ERROR,
            };
        }
        return true;
    }

    async createDatabase(dbName: string): Promise<boolean> {
        const client = new MongoClient(MONGO_URI);
        let creation = false;

        try {
            await client.connect();
            const existingDb = await client.db().admin().listDatabases();
            const dbExists = existingDb.databases.some((db) => db.name.toLowerCase() === dbName.toLowerCase());

            if (!dbExists) {
                const db = client.db(dbName.toUpperCase());
                const collectionsToCreate = Object.values(TABLE_NAMES).filter(
                    (collection) => collection !== TABLE_NAMES.TENANT_SETUP && collection !== TABLE_NAMES.DEFAULT_EXPORT && collection !== TABLE_NAMES.USER
                );
                await Promise.all(collectionsToCreate.map((collection) => db.createCollection(collection)));
                creation = true;
            } else {
                // Database already exists, no need to create it
            }
            return creation;
        } catch (error) {
            return creation;
        } finally {
            await client.close();
        }
    }

    async cloneAndUpdate(
        collectionName: string,
        filter: any,
        updateFields: any,
        dbName?: string,
        options?: FindOneAndUpdateOptions & { arrayFilters?: any[] },
        session?: ClientSession
    ): Promise<any> {
        try {
            // Fetch the document to clone
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const documentToClone = await collection.findOne(filter, { session });

            if (!documentToClone) {
                throw new Error('No document found with the provided filter');
            }

            // Clone the document
            const clonedDocument = { ...documentToClone };
            delete clonedDocument._id;

            // Update the fields
            for (const key in updateFields) {
                if (key === 'data' && documentToClone[key]) {
                    // Merge the data objects
                    clonedDocument[key] = { ...documentToClone[key], ...updateFields[key] };
                } else {
                    clonedDocument[key] = updateFields[key];
                }
            }

            // Insert the cloned and updated document back into the collection
            const insertedDocument = await collection.insertOne(clonedDocument, { session });

            return insertedDocument;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateMany(collectionName: string, filter: object, update: object, schema?: string, session?: ClientSession) {
        try {
            const db = this.client.db(schema);
            const collection = db.collection(collectionName);
            const result = await collection.updateMany(filter, update, { session });
            return result;
        } catch (error) {
            return null;
        }
    }

    async deleteDatabase(dbName: string): Promise<void> {
        try {
            if (dbName.toUpperCase() === MONGO_DB_NAME) {
                return
            }
            else {
                const db = this.client.db(dbName);
                await db.dropDatabase();
            }
        } catch (error) {
            throw error;
        }
    }

    async copyCollectionToAnotherDB(
        sourceTenantId: string,
        targetTenantId: string,
        sourceDbName: string,
        targetDbName: string,
        collectionName: string
    ): Promise<void> {
        try {
            const sourceDb = this.client.db(sourceDbName);
            const targetDb = this.client.db(targetDbName);

            const pipeline = [
                {
                    $out: { db: targetDbName, coll: collectionName },
                },
            ];

            await sourceDb.collection(collectionName).aggregate(pipeline).toArray();
        } catch (error) {
            throw error;
        }
    }

    async copyAndModifyDocuments(
        sourceTenantId: string,
        targetTenantId: string,
        sourceDbName: string,
        targetDbName: string,
        collectionName: string,
        isMulti: boolean
    ): Promise<void> {
        try {
            const sourceDb = this.client.db(sourceDbName);
            const targetDb = this.client.db(targetDbName);
    
            const filter = { tenant_id: sourceTenantId };
            const documents = await sourceDb
                .collection(collectionName)
                .find(filter)
                .toArray();
    
            if (isMulti) {
                const modifiedDocuments = documents.map((doc) => ({
                    ...doc,
                    _id: new ObjectId(),
                    tenant_id: targetTenantId,
                }));
                if (modifiedDocuments.length > 0) {
                    await targetDb.collection(collectionName).insertMany(modifiedDocuments);
                }
            } else {
                if (documents.length > 0) {
                    const modifiedDocument = {
                        ...documents[0],
                        _id: new ObjectId(),
                        tenant_id: targetTenantId,
                    };
                    await targetDb.collection(collectionName).insertOne(modifiedDocument);
                }
            }
        } catch (error) {
            throw error;
        }
    }

    async cloneTenant(source_tenant_id: string, target_tenant_id: string, source_tenant_schema: string, target_tenant_schema: string,newTenantName:string): Promise<any> {
        try {
            const sourceDb = this.client.db(source_tenant_schema);
            const targetDb = this.client.db(target_tenant_schema);
            const db = this.client.db(MONGO_DB_NAME);
            const collections = await sourceDb.listCollections().toArray();

            for (const collection of collections) {
                const collectionName = collection.name;    
                if (collectionName === TABLE_NAMES.TENANT_SETUP || collectionName === TABLE_NAMES.ERROR_LOGS || collectionName === TABLE_NAMES.EMAIL_TRAIL || collectionName === TABLE_NAMES.TRAIL_TABLE || collectionName === TABLE_NAMES.DEFAULT_EXPORT|| collectionName===TABLE_NAMES.USER ) {
                    continue;
                }
                else if (collectionName === TABLE_NAMES.FACILITY_MASTER) {
                    const filter = { tenant_id: source_tenant_id };
                    const documents = await sourceDb.collection(collectionName).find(filter).toArray();
                    if (documents.length > 0) {
                        const modifiedDocuments = documents.map((doc) => ({
                            
                            ...doc,
                            _id: new ObjectId(),
                            tenant_id: target_tenant_id,
                            tenant_name:newTenantName
                        }));
                        try{
                            await targetDb.collection(collectionName).insertOne(modifiedDocuments[0]);
                        }catch(err){
                        }
                        try{
                            await db.collection(collectionName).insertOne(modifiedDocuments[0]);
                        }catch(err){    
                        }
                    }   
                }
                const hasDocumentWithTenantId = await sourceDb.collection(collectionName).findOne({ tenant_id: { $exists: true } });
                if (hasDocumentWithTenantId) {
                    const filter = { tenant_id: source_tenant_id };
                    const documents = await sourceDb.collection(collectionName).find(filter).toArray();
                    if (documents.length > 0) {
                        const modifiedDocuments = documents.map((doc) => ({
                            ...doc,
                            tenant_id: target_tenant_id,
                            tenant_name:newTenantName
                        }));
                        await targetDb.collection(collectionName).insertMany(modifiedDocuments);
                    }
                } else if (source_tenant_schema !== target_tenant_schema) {
                    await this.copyCollectionToAnotherDB(source_tenant_id, target_tenant_id, source_tenant_schema, target_tenant_schema, collectionName);
                }
            }
            return true;
        } catch (error) {
            return false;
        }
    }

    async updateFacilityNameInDocuments(
        tenantId: string,
        dbName: string,
        collectionName: string,
        oldFacilityName: string,
        newFacilityName: string
    ): Promise<void> {
        try {
            const db = this.client.db(dbName);
            const filter = { tenant_id: tenantId, 'data.facility_name': oldFacilityName };
            const update = { $set: { 'data.$[].facility_name': newFacilityName } };
    
            const result = await db.collection(collectionName).updateMany(filter, update);
    
        } catch (error) {
            throw error;
        }
    }

    async collectionExists(collectionName: string, dbName?: string, session?: ClientSession): Promise<boolean> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collections = await db.listCollections({ name: collectionName }).toArray();
            return collections.length > 0;
        } catch (error) {
            throw new Error(`Error checking if collection exists: ${error.message}`);
        }
    }


    async createCollection(collectionName: string,dbName?: string, session?: ClientSession): Promise<void> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            await db.createCollection(collectionName);
        } catch (error) {
            throw new Error(`Error creating collection: ${error.message}`);
        }
    }



    async findOne(collectionName: string, filter: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.findOne(filter, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateSpecificSlotObject(
        filter:any,
        timeIndex: number,
        updateValues: any,
        dbName?: string,
        session?: ClientSession
    ): Promise<any> {
        // const session = await this.client.startSession();
    
        try {
            // session.startTransaction();
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(TABLE_NAMES.PROVIDER_APPOINTMENT_DETAILS);
    
            const update = {
                $set: {
                    "data.$[element]": updateValues
                }
            };
    
            const options = {
                arrayFilters: [{ 'element.time': updateValues.time }],session,returnDocument: ReturnDocument.AFTER
            };
    
            let result;
            result = await collection.findOneAndUpdate(filter, update, options); 
            // await session.commitTransaction();
            return result;
        } catch (error) {
            // await session.abortTransaction();
            throw new Error(error.message);
        } 
    }

    async deleteOne(collectionName: string, filter: any, dbName?: string, session?: ClientSession): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const res = await collection.deleteOne(filter, { session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async oldfindOneAndUpdate(
        collectionName: string,
        filter: any,
        update: any,
        dbName?: string,
        options?: any,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const result = await collection.findOneAndUpdate(filter, update, {...options,session});
            if (!result) {
                throw new Error('No document found with the provided filter');
            }
            return result;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async getFile(dbName:string,id:string){
        try {
            const db = this.client.db(dbName);
            const collection = db.collection(TABLE_NAMES.ASSETS);
            const result = await collection.findOne({ _id: new ObjectId(id) });
            if (result) {
                const filename = result.filename;
                const data = result.data.buffer;
                // const mimeType = this.getMimeType(filename);
                return data;
            } else {
                return {
                    statusCode: 400,
                    message: "No record found"
                }
                throw new Error(`File with ID ${id} not found in MongoDB collection myCollection`);
            }
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateItemQuery1(
        collectionName: string,
        filter: any,
        updateValues: any,
        dbName?: string,
        options?: any,
        session?: ClientSession
    ): Promise<any> {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const update = { $set: updateValues };
            const collection = db.collection(collectionName);
            const res = await collection.updateOne(filter, update, { ...options, session });
            return res;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async createView(viewName:string,collectionName:string,pipeline?:any[],dbName?:string){
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const result=await db.createCollection(
                viewName,
                {
                    viewOn:collectionName,
                    pipeline:pipeline
                }
            )
            return result
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async dropView(viewName:string,dbName?:string){
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(viewName);
            const result=await collection.drop();
            return result;
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async updateView(viewName:string,collectionName:string,pipeline?:any[],dbName?:string){
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const result=await db.command({
                collMod:viewName,
                viewOn:collectionName,
                pipeline:pipeline
            })
            return result;
        } catch (error) {
            throw new Error(error.message); 
        }
    }

    async viewExists(viewName:string,dbName?:string){
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collections = await db.listCollections().toArray();
            return collections.some(col => col.name === viewName && col.type === 'view');
        } catch (error) {
            throw new Error(error.message); 
        }
    }

    async getAllItemFromView(collectionName:string,dbName?:string){
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collection = db.collection(collectionName);
            const result=collection.find({}).toArray();
            return result;
        } catch (error) {
            throw new Error(error.message);
        }
    }
      

    private getMimeType(filename: string): string {
        const extension = filename.split('.').pop();
        switch (extension) {
          case 'pdf':
            return 'application/pdf';
          case 'jpg':
          case 'jpeg':
            return 'image/jpeg';
          case 'png':
            return 'image/png';
          default:
            return 'application/octet-stream';
        }
    }

    async listIndexes(dbName: string, collectionName: string) {
        try {
            const db = dbName ? this.client.db(dbName) : this.db;
            const collections = await db.listCollections({ name: collectionName }).toArray();
            if (collections.length === 0) {
                await db.createCollection(collectionName);
            }
            const indexes = await db.collection(collectionName).listIndexes().toArray();
            return indexes;
        } catch (error) {
            throw new Error(`Failed to list indexes: ${error.message}`);
        }
    }


    async getImage(id: string, dbName: string): Promise<{ dataUrl: string } | { statusCode: number, message: string }> {
        try {
            const db = this.client.db(dbName);
            const collection = db.collection(TABLE_NAMES.ASSETS);
            const result = await collection.findOne({ _id: new ObjectId(id) });
            if (result) {
                const filename = result.filename;
                const data = result.data.buffer;
                const mimeType = this.getMimeType(filename);
                const base64String = data.toString('base64');
                const dataUrl = `data:${mimeType};base64,${base64String}`;
                return { dataUrl };
            } else {
                return {
                    statusCode: 400,
                    message: "No record found"
                };
            }
        } catch (error) {
            throw new Error(error.message);
        }
    }

    async getDataStream(collectionName: string, filter?: object, dbName?: string): Promise<AsyncIterable<any>> {
        const db = dbName ? this.client.db(dbName) : this.db;
        return db.collection(collectionName).find(filter).stream();
    }
      
    
}

// function getConnectionURI(network: string): string {
//     switch (network) {
//         case 'DEV':
//             return MONGO_URI_DEV;
//         case 'TEST':
//             return MONGO_URI_TEST;
//         case 'DEMO':
//             return MONGO_URI_DEMO;
//         default:
//             throw new Error('Invalid network specified');
//     }
// }

// function getConnectionOptions(network: string): { maxPoolSize: number; minPoolSize: number; maxIdleTimeMS: number; socketTimeoutMS: number } {
//     switch (network) {
//         case 'DEV':
//             return {
//                 maxPoolSize: 200,
//                 minPoolSize: 10,
//                 maxIdleTimeMS: 60000,
//                 socketTimeoutMS: 30000,
//             };
//         case 'TEST':
//             return {
//                 maxPoolSize: 800,
//                 minPoolSize: 20,
//                 maxIdleTimeMS: 60000,
//                 socketTimeoutMS: 30000,
//             };
//         case 'DEMO':
//             return {
//                 maxPoolSize: 200,
//                 minPoolSize: 15,
//                 maxIdleTimeMS: 60000,
//                 socketTimeoutMS: 30000,
//             };
//         default:
//             throw new Error('Invalid network specified');
//     }
// }


