module org.xbib.io.jdbc.pool {
    exports org.xbib.io.pool.jdbc;
    exports org.xbib.io.pool.jdbc.util;
    requires java.logging;
    requires transitive java.sql;
}
