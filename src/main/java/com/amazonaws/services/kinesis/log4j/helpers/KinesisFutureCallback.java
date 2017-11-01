package com.amazonaws.services.kinesis.log4j.helpers;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;

/**
 * Created by gaurav on 10/27/17.
 */
public class KinesisFutureCallback implements FutureCallback<UserRecordResult> {

    private String data, appenderName;
    private Logger logger,failedRecordLogger;
    public KinesisFutureCallback(String data, String appenderName,Logger logger, Logger failedRecordLogger){
        this.appenderName = appenderName;
        this.data = data;
        this.logger = logger;
        this.failedRecordLogger = failedRecordLogger;
    }

    @Override
    public void onSuccess(UserRecordResult result) {
        if (logger.isDebugEnabled() && (result.getSequenceNumber().endsWith("0000"))) {
            logger.debug("Appender (" + appenderName + ") added " + data
                    + " to the stream");
        }
    }

    @Override
    public void onFailure(Throwable throwable) {
        if (throwable instanceof UserRecordFailedException) {
            Attempt last = Iterables.getLast(
                    ((UserRecordFailedException) throwable).getResult().getAttempts());
            logger.error(String.format(
                    "Record failed to put using Appender %s - %s : %s",
                    appenderName,last.getErrorCode(), last.getErrorMessage()));
        }
        failedRecordLogger.error(data);
    }
}
