package uk.dioxic.mtdk.cli.command;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(mixinStandardHelpOptions = true, version = "Mongo TDK v0.0.1")
public class MongoClientCommand {
    final Logger logger = LoggerFactory.getLogger(getClass());

    @Option(names = {"-d", "--database"}, description = "mongo database (default: ${DEFAULT-VALUE})", defaultValue = "test")
    String database;

    @Option(names = {"-c", "--collection"}, description = "mongo collection", required = true)
    String collection;

    @Option(names = {"-u", "--uri"}, description = "mongoUri (default: ${DEFAULT-VALUE})", defaultValue = "mongodb://localhost:27017")
    String mongoUri;

    private MongoClient asyncClient;

    private com.mongodb.client.MongoClient syncClient;

    public MongoClientCommand() {
    }

    public MongoClient getAsyncClient() {
        if (asyncClient == null) {
            asyncClient = MongoClients.create(mongoUri);
        }
        return asyncClient;
    }

    public com.mongodb.client.MongoClient getSyncClient() {
        if (syncClient == null) {
            syncClient = com.mongodb.client.MongoClients.create(mongoUri);
        }
        return syncClient;
    }

    public MongoClient createAsyncClient(MongoClientSettings.Builder mcsBuilder) {
        asyncClient = MongoClients.create(applyConnectionString(mcsBuilder).build());
        return asyncClient;
    }

    public com.mongodb.client.MongoClient createSyncClient(MongoClientSettings.Builder mcsBuilder) {
        syncClient = com.mongodb.client.MongoClients.create(applyConnectionString(mcsBuilder).build());
        return syncClient;
    }

    public MongoClientSettings.Builder applyConnectionString(MongoClientSettings.Builder mcsBuilder) {
        return mcsBuilder.applyConnectionString(new ConnectionString(mongoUri));
    }

    public MongoDatabase getAsyncDatabase() {
        return getAsyncDatabase(database);
    }

    public com.mongodb.client.MongoDatabase getSyncDatabase() {
        return getSyncDatabase(database);
    }

    public MongoDatabase getAsyncDatabase(String database) {
        return getAsyncClient().getDatabase(database);
    }

    public com.mongodb.client.MongoDatabase getSyncDatabase(String database) {
        return getSyncClient().getDatabase(database);
    }

    public <TDocument> MongoCollection<TDocument> getAsyncCollection(Class<TDocument> clazz) {
        return getAsyncDatabase(database).getCollection(collection, clazz);
    }

    public MongoCollection<Document> getAsyncCollection() {
        return getAsyncCollection(Document.class);
    }

    public <TDocument> com.mongodb.client.MongoCollection<TDocument> getSyncCollection(Class<TDocument> clazz) {
        return getSyncDatabase(database).getCollection(collection, clazz);
    }

    public com.mongodb.client.MongoCollection<Document> getSyncCollection() {
        return getSyncCollection(Document.class);
    }
}
