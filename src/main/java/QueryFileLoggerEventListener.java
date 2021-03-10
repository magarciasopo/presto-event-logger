import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.util.logging.Logger;

/**
 * Created by dharmeshkakadia on 4/19/2017.
 */
public class QueryFileLoggerEventListener implements EventListener{
	 Logger logger;
	 FileHandler fh;
	 final String loggerName = "QueryLog";
	
	public QueryFileLoggerEventListener(Map<String, String> config){
		createLogFile();
	}

	public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
		String errorCode = null;
        StringBuilder msg = new StringBuilder();

        try {
            errorCode = queryCompletedEvent.getFailureInfo().get().getErrorCode().getName().toString();
        }
        catch (NoSuchElementException noElEx) {
            errorCode = null;
        }

        try {
                msg.append(queryCompletedEvent.getMetadata().getQueryId().toString());
                msg.append(";");
                msg.append(queryCompletedEvent.getContext().getUser().toString());
                msg.append(";");
                msg.append(queryCompletedEvent.getMetadata().getQuery().toString().replace("\n", " "));
                msg.append(";");
                msg.append(queryCompletedEvent.getCreateTime());
                msg.append(";");
                msg.append(queryCompletedEvent.getEndTime());
                msg.append(";");
                msg.append(queryCompletedEvent.getStatistics().isComplete());
                msg.append(";");
                msg.append(errorCode);
                logger.info(msg.toString());
        }
        catch (Exception ex) {
           // logger.info(ex.getMessage());
        }
	}

	public void createLogFile()
    {

        SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String timeStamp = dateTime.format(new Date());
        StringBuilder logPath = new StringBuilder();

        logPath.append("/var/log/presto/queries");
        logPath.append(".%g.log");

        try {
            logger = Logger.getLogger(loggerName);
            fh = new FileHandler(logPath.toString(), 524288000, 5, true);
            logger.addHandler(fh);
            logger.setUseParentHandlers(false);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        }
        catch (IOException e) {
            logger.info(e.getMessage());
        }
    }
}
