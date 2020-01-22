package de.th_koeln.iws.sh2.aggregation;

/**
 *
 */

import static de.th_koeln.iws.sh2.aggregation.core.util.TableNames.LOGS;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.message.ParameterizedMessage;

import de.th_koeln.iws.sh2.aggregation.core.AffiliationAggregator;
import de.th_koeln.iws.sh2.aggregation.core.AffiliationWrapper;
import de.th_koeln.iws.sh2.aggregation.core.ApacheLogAggregator;
import de.th_koeln.iws.sh2.aggregation.core.BhtDbWrapper;
import de.th_koeln.iws.sh2.aggregation.core.CitationAggregator;
import de.th_koeln.iws.sh2.aggregation.core.CitationWrapper;
import de.th_koeln.iws.sh2.aggregation.core.DelayWrapper;
import de.th_koeln.iws.sh2.aggregation.core.GeolocationWrapper;
import de.th_koeln.iws.sh2.aggregation.core.HdblpDbWrapper;
import de.th_koeln.iws.sh2.aggregation.core.InternationalityAggregator;
import de.th_koeln.iws.sh2.aggregation.core.ProminenceAggregator;
import de.th_koeln.iws.sh2.aggregation.core.RatingAggregator;
import de.th_koeln.iws.sh2.aggregation.core.RecordDbWrapper;
import de.th_koeln.iws.sh2.aggregation.core.SizeAggregator;
import de.th_koeln.iws.sh2.aggregation.core.StreamDbWrapper;
import de.th_koeln.iws.sh2.aggregation.core.helper.PythonGeolocationFinder;
import de.th_koeln.iws.sh2.aggregation.db.DatabaseManager;

/**
 * @author michels
 *
 */
public class AggregateApplication {

    private static Logger LOGGER = LogManager.getLogger(AggregateApplication.class);

    private static void setRootLoggerLevel(Level level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        setRootLoggerLevel(Level.INFO);
        final int lteYear = 2017;
        final int evalYear = 2018;

        final String wrapLogFormat = "{}: Reading and processing dblp resources via its interfaces";
        final Instant wrapStart = Instant.now();
        LOGGER.info(wrapLogFormat, "START");

        try (final Connection connection = DatabaseManager.getInstance().getConnection()) {

            HdblpDbWrapper.wrapTo(connection);

            StreamDbWrapper.wrapTo(connection);
            BhtDbWrapper.wrapTo(connection);
            RecordDbWrapper.wrapTo(connection, lteYear);

            GeolocationWrapper.wrapTo(connection, new PythonGeolocationFinder());
            CitationWrapper.wrapTo(connection, lteYear);
            AffiliationWrapper.wrapTo(connection);

            /*
             * benötigt zuvor ausgeführtes Projekt apache-log-analysis:
             */
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m03");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m04");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m05");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m06");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m07");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m08");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m09");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m10");
            // ApacheLogWrapper.wrapTo(connection, LOGS, "dblp_logs_y2018m11");
            // =========================================================================

            SizeAggregator.wrapTo(connection, lteYear);
            RatingAggregator.wrapTo(connection);
            ProminenceAggregator.wrapTo(connection, lteYear);

            InternationalityAggregator.wrapTo(connection, lteYear);
            CitationAggregator.wrapTo(connection, lteYear);
            AffiliationAggregator.wrapTo(connection, lteYear);

            ApacheLogAggregator.wrapTo(connection, LOGS);

            DelayWrapper.wrapTo(connection, evalYear);

            /*
             * TODO: finally create a view for all the scores per stream
             * 
             * create materialized view dblp_stream_scores as select streams.stream_key,
             * affil.affil_score, cite.citation_score, ints.intl_score,
             * prom.prominence_score, sizes.size_score, rates.rating_score,
             * logs.log_score_y2018m03, logs.log_score_y2018m04, logs.log_score_y2018m05,
             * logs.log_score_y2018m06, logs.log_score_y2018m07, logs.log_score_y2018m08,
             * logs.log_score_y2018m09, logs.log_score_y2018m10, logs.log_score_y2018m11
             * from dblp_conference_streams streams left join
             * dblp_conference_raw_affil_scores affil on streams.stream_key =
             * affil.stream_key left join dblp_conference_raw_citation_scores cite on
             * streams.stream_key = cite.stream_key left join
             * dblp_conference_raw_intl_scores ints on streams.stream_key = ints.stream_key
             * left join dblp_conference_raw_prominence_scores prom on streams.stream_key =
             * prom.stream_key left join dblp_conference_raw_size_scores sizes on
             * streams.stream_key = sizes.stream_key left join
             * dblp_conference_raw_rating_scores rates on streams.stream_key =
             * rates.stream_key left join dblp_conference_raw_log_scores logs on
             * streams.stream_key = logs.stream_key;
             */

        } catch (SQLException e) {
            LOGGER.fatal(new ParameterizedMessage(wrapLogFormat, "FAILED"), e);
            System.exit(1);
        }
        LOGGER.info(wrapLogFormat + " (Duration: {})", "END", Duration.between(wrapStart, Instant.now()));
    }

}
