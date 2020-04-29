package uk.dioxic.mtdk.cli.mixin;

import com.mongodb.*;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.Data;
import lombok.Getter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("FieldMayBeFinal")
@Command
public abstract class MongoCommonMixin {
    final Logger logger = LoggerFactory.getLogger(getClass());

    @Mixin
    private FormattingMixin formattingMixin;

    @Getter
    @Option(names = {"-d", "--database"}, description = "database to use (default: ${DEFAULT-VALUE})", defaultValue = "test", paramLabel = "arg")
    private String database;

    @Getter
    @Option(names = {"-c", "--collection"}, description = "collection to use", required = true, paramLabel = "arg")
    private String collection;

    @Getter
    @ArgGroup(validate = false, heading = "%nAuthentication Options:%n", exclusive = false)
    private AuthenticationOptions authenticationOptions;

    @Getter
    @ArgGroup(validate = false, heading = "%nConnection Options:%n")
    private ConnectionOptions connectionOptions = new ConnectionOptions();

    private static class ConnectionOptions {
        @Option(names = {"--host"}, description = "server to connect to (default: ${DEFAULT-VALUE})", defaultValue = "localhost", paramLabel = "arg")
        private String host;

        @Option(names = {"--port"}, description = "port to connect to (default: ${DEFAULT-VALUE})", defaultValue = "27017", paramLabel = "arg")
        private Integer port;

        @Option(names = {"--tls"}, description = "use TLS for connections (default: ${DEFAULT-VALUE})", defaultValue = "false")
        private Boolean tls;

        @Option(names = {"--tlsAllowInvalidHostnames"}, description = "allow connections to servers with non-matching hostnames (default: ${DEFAULT-VALUE})", defaultValue = "false")
        private Boolean allowInvalidHostnames;

        @Option(names = {"--uri"}, description = "mongo URI (default: ${DEFAULT-VALUE})", defaultValue = "mongodb://localhost:27017", paramLabel = "arg")
        private String uri;

        void apply(MongoClientSettings.Builder mcsBuilder) {
            if (uri != null) {
                mcsBuilder.applyConnectionString(new ConnectionString(uri));
            } else {
                mcsBuilder.applyToClusterSettings(builder -> builder.hosts(Collections.singletonList(new ServerAddress(host, port))));
            }

            mcsBuilder.applyToSslSettings(builder -> {
                builder.enabled(tls);
                builder.invalidHostNameAllowed(allowInvalidHostnames);
            });
        }
    }

    private static class AuthenticationOptions {
        private enum AuthMechanism { gssapi, x509, scramsha, ldap }

        @Option(names = {"-u", "--username"}, description = "username for authentication", paramLabel = "arg")
        private String username;

        @Option(names = {"-p", "--password"}, description = "password for authentication", interactive = true, arity = "0..1", paramLabel = "arg")
        private char[] password;

        @Option(names = {"--authenticationDatabase"}, description = "user source (default: ${DEFAULT-VALUE})", defaultValue = "admin", paramLabel = "arg")
        private String authenticationDatabase;

        @Option(names = {"--authenticationMechanism"}, description = "authentication mechanism, one of ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", defaultValue = "scramsha", paramLabel = "arg")
        private AuthMechanism authenticationMechanism;

        void apply(MongoClientSettings.Builder mcsBuilder) {
            if (hasCredential()) {
                mcsBuilder.credential(MongoCredential.createCredential(username,authenticationDatabase, password));
            }
        }

        boolean hasCredential() {
            return username != null && password != null && authenticationDatabase != null;
        }
    }

    private MongoClient asyncClient;

    private com.mongodb.client.MongoClient syncClient;

    private List<Function<MongoClientSettings.Builder,MongoClientSettings.Builder>> clientSettingFunctions = new ArrayList<>();

    public MongoClient getAsyncClient() {
        if (asyncClient == null) {
            asyncClient = MongoClients.create(getMongoClientSettings());
        }
        return asyncClient;
    }

    public com.mongodb.client.MongoClient getSyncClient() {
        if (syncClient == null) {
            syncClient = com.mongodb.client.MongoClients.create(getMongoClientSettings());
        }
        return syncClient;
    }

    private MongoClientSettings getMongoClientSettings() {
        MongoClientSettings.Builder mcsBuilder = MongoClientSettings.builder();

        mcsBuilder.applicationName("mongo-tdk");
        connectionOptions.apply(mcsBuilder);

        if (authenticationOptions != null) {
            authenticationOptions.apply(mcsBuilder);
        }

        clientSettingFunctions.forEach(func -> func.apply(mcsBuilder));

        return mcsBuilder.build();
    }

    public void applyToClientSettings(Function<MongoClientSettings.Builder,MongoClientSettings.Builder> builderFunction) {
        clientSettingFunctions.add(builderFunction);
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
