package com.multicloud.batch.job;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public class DateRangePartition {

    // Partition must be under a month
    public static List<CustomDateRange> getPartitions(long remainingDays, long interval) {

        List<CustomDateRange> partitions = new ArrayList<>();

        LocalDate endDate = LocalDate.now();

        // This month
        long dayOfMonth = endDate.getDayOfMonth();
        endDate = makePartitionRange(Math.min(dayOfMonth, remainingDays), endDate, interval, partitions);
        remainingDays -= dayOfMonth;

        // Remaining
        YearMonth month = YearMonth.now().minusMonths(1);

        while (remainingDays > 0) {

            long len = month.lengthOfMonth();

            endDate = makePartitionRange(Math.min(len, remainingDays), endDate, interval, partitions);
            remainingDays -= len;

            month = month.minusMonths(1);

        }

        return partitions;
    }

    private static LocalDate makePartitionRange(long len, LocalDate endDate, long interval, List<CustomDateRange> partitions) {

        while (len > 0) {

            long min = Math.min(len, interval);

            LocalDate start = endDate.minusDays(min - 1);

            partitions.add(new CustomDateRange(start, endDate, endDate.getYear(), endDate.getMonthValue()));

            len -= min;
            endDate = start.minusDays(1);

        }

        return endDate;
    }

}
