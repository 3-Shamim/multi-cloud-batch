package com.multicloud.batch.job;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record CustomDateRange(LocalDate start, LocalDate end, int year, int month) implements Serializable {
}