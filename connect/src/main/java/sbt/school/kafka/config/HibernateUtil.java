package sbt.school.kafka.config;

import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import sbt.school.kafka.entity.Transaction;

public class HibernateUtil {

    public static SessionFactory getSessionFactory(DBSourceConfig config) {
        return new MetadataSources(
                new StandardServiceRegistryBuilder()
                        .applySetting("hibernate.connection.url", config.getString(DBSourceConfig.DB_URL))
                        .applySetting("hibernate.connection.username", config.getString(DBSourceConfig.DB_USER))
                        .applySetting("hibernate.connection.password", config.getString(DBSourceConfig.DB_PASSWORD))
                        .applySetting("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect")
                        .build())
                .addAnnotatedClass(Transaction.class)
                .buildMetadata()
                .buildSessionFactory();
    }
}
