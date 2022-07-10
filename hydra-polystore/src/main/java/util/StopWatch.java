package util;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StopWatch {

    /* Private Instance Variables */
    /** Stores the start time when an object of the StopWatch class is initialized. */
    LocalDateTime start, end;

    /**
     * Custom constructor which initializes the {@link #startTime} parameter.
     */
    public StopWatch() {
        start = LocalDateTime.now();
    }

    /**
     * Gets the elapsed time (in seconds) since the time the object of StopWatch was initialized.
     * 
     * @return Elapsed time in seconds.
     */
    public double getElapsedTimeInSeconds() {
		end = LocalDateTime.now();
        return Duration.between(start,end).getSeconds();
    }
}
